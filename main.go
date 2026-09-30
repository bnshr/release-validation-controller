package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var groupVersion = schema.GroupVersion{Group: "learning.example.io", Version: "v1alpha1"}

type ValidationRunSpec struct {
	Version        string `json:"version"`
	Suite          string `json:"suite"`
	Capacity       string `json:"capacity,omitempty"`
	TimeoutSeconds int64  `json:"timeoutSeconds,omitempty"`
	Retries        int32  `json:"retries,omitempty"`
	Cancel         bool   `json:"cancel,omitempty"`
}
type ValidationRunStatus struct {
	Phase              string       `json:"phase,omitempty"`
	Attempt            int32        `json:"attempt,omitempty"`
	StartedAt          *metav1.Time `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time `json:"completedAt,omitempty"`
	Reason             string       `json:"reason,omitempty"`
	Report             string       `json:"report,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
}
type ValidationRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ValidationRunSpec   `json:"spec,omitempty"`
	Status            ValidationRunStatus `json:"status,omitempty"`
}
type ValidationRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValidationRun `json:"items"`
}

func (r *ValidationRun) DeepCopyObject() runtime.Object {
	out := new(ValidationRun)
	*out = *r
	out.ObjectMeta = *r.ObjectMeta.DeepCopy()
	if r.Status.StartedAt != nil {
		t := r.Status.StartedAt.DeepCopy()
		out.Status.StartedAt = t
	}
	if r.Status.CompletedAt != nil {
		t := r.Status.CompletedAt.DeepCopy()
		out.Status.CompletedAt = t
	}
	return out
}
func (r *ValidationRunList) DeepCopyObject() runtime.Object {
	out := new(ValidationRunList)
	*out = *r
	out.ListMeta = r.ListMeta
	out.Items = make([]ValidationRun, len(r.Items))
	for i := range r.Items {
		out.Items[i] = *(r.Items[i].DeepCopyObject().(*ValidationRun))
	}
	return out
}

type Reconciler struct {
	client.Client
	Scheme                   *runtime.Scheme
	Namespace                string
	CPUCapacity, GPUCapacity int
}

const finalizer = "learning.example.io/release-allocation"
const allocationName = "validation-capacity"

func terminal(p string) bool {
	return p == "Passed" || p == "Failed" || p == "Cancelled" || p == "TimedOut"
}
func class(r *ValidationRun) string {
	if r.Spec.Capacity == "gpu-simulated" {
		return "gpu-simulated"
	}
	return "cpu"
}
func jobName(r *ValidationRun, attempt int32) string {
	base := r.Name
	if len(base) > 45 {
		base = base[:45]
	}
	return fmt.Sprintf("%s-%s-a%d", base, string(r.UID)[:min(8, len(r.UID))], attempt)
}

func (c *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r := &ValidationRun{}
	if err := c.Get(ctx, req.NamespacedName, r); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Namespace != c.Namespace {
		return ctrl.Result{}, nil
	}
	if r.DeletionTimestamp != nil {
		return ctrl.Result{}, c.cleanup(ctx, r)
	}
	if terminal(r.Status.Phase) {
		return ctrl.Result{}, c.cleanup(ctx, r)
	}
	if !controllerutil.ContainsFinalizer(r, finalizer) {
		controllerutil.AddFinalizer(r, finalizer)
		return ctrl.Result{}, c.Update(ctx, r)
	}
	if r.Spec.Cancel {
		return ctrl.Result{}, c.finish(ctx, r, "Cancelled", "Cancellation requested")
	}
	if r.Spec.Version == "" || r.Spec.Suite == "" || (r.Spec.Capacity != "" && r.Spec.Capacity != "cpu" && r.Spec.Capacity != "gpu-simulated") {
		return ctrl.Result{}, c.finish(ctx, r, "Failed", "Invalid version, suite, or capacity")
	}
	if r.Status.StartedAt != nil && r.Spec.TimeoutSeconds > 0 && time.Since(r.Status.StartedAt.Time) >= time.Duration(r.Spec.TimeoutSeconds)*time.Second {
		return ctrl.Result{}, c.finish(ctx, r, "TimedOut", "Run timeout exceeded")
	}
	got, err := c.allocate(ctx, r)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !got {
		if r.Status.Phase != "Queued" {
			if err := c.setStatus(ctx, r, "Queued", "Waiting for "+class(r)+" capacity", ""); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if r.Status.StartedAt == nil {
		now := metav1.Now()
		r.Status.StartedAt = &now
	}
	if r.Status.Attempt == 0 {
		r.Status.Attempt = 1
	}
	job := &batchv1.Job{}
	err = c.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: jobName(r, r.Status.Attempt)}, job)
	if apierrors.IsNotFound(err) {
		job = c.makeJob(r)
		if err = c.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, c.setStatus(ctx, r, "Running", fmt.Sprintf("Attempt %d started", r.Status.Attempt), "")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !metav1.IsControlledBy(job, r) {
		return ctrl.Result{}, fmt.Errorf("job %s exists but is not owned by validation run %s", job.Name, r.Name)
	}
	if job.Status.Succeeded > 0 {
		return ctrl.Result{}, c.finish(ctx, r, "Passed", fmt.Sprintf("Suite %s passed on attempt %d", r.Spec.Suite, r.Status.Attempt))
	}
	if job.Status.Failed > 0 && job.Status.Active == 0 {
		if r.Status.Attempt <= r.Spec.Retries {
			r.Status.Attempt++
			return ctrl.Result{RequeueAfter: time.Second}, c.setStatus(ctx, r, "Running", fmt.Sprintf("Retrying after failed attempt %d", r.Status.Attempt-1), "")
		}
		return ctrl.Result{}, c.finish(ctx, r, "Failed", fmt.Sprintf("Suite %s failed after %d attempt(s)", r.Spec.Suite, r.Status.Attempt))
	}
	if r.Status.Phase != "Running" {
		if err := c.setStatus(ctx, r, "Running", fmt.Sprintf("Attempt %d running", r.Status.Attempt), ""); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func (c *Reconciler) makeJob(r *ValidationRun) *batchv1.Job {
	cmd := "echo Validating version=$VERSION suite=$SUITE capacity=$CAPACITY; sleep 2; case $SUITE in smoke|network|storage|gpu-smoke) exit 0;; fail) exit 1;; *) echo unknown-suite >&2; exit 2;; esac"
	backoff := int32(0)
	j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName(r, r.Status.Attempt), Namespace: r.Namespace, Labels: map[string]string{"app": "release-validation-controller", "validation-run": r.Name}}, Spec: batchv1.JobSpec{BackoffLimit: &backoff, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "validate", Image: "busybox:1.36", Command: []string{"/bin/sh", "-c", cmd}, Env: []corev1.EnvVar{{Name: "VERSION", Value: r.Spec.Version}, {Name: "SUITE", Value: r.Spec.Suite}, {Name: "CAPACITY", Value: class(r)}}}}}}}}
	_ = controllerutil.SetControllerReference(r, j, c.Scheme)
	return j
}

func (c *Reconciler) setStatus(ctx context.Context, r *ValidationRun, phase, reason, report string) error {
	r.Status.Phase, r.Status.Reason, r.Status.Report = phase, reason, report
	r.Status.ObservedGeneration = r.Generation
	return c.Status().Update(ctx, r)
}
func (c *Reconciler) finish(ctx context.Context, r *ValidationRun, phase, reason string) error {
	now := metav1.Now()
	r.Status.CompletedAt = &now
	report := fmt.Sprintf("Learning project qualification: %s. Version %s; suite %s; capacity %s; attempts %d. Reason: %s. Jobs use scripted CPU containers; gpu-simulated reserves a logical slot and does not test GPU hardware.", strings.ToUpper(phase), r.Spec.Version, r.Spec.Suite, class(r), r.Status.Attempt, reason)
	// Persist the decision first. A failed cleanup can then safely resume on the next reconcile.
	return c.setStatus(ctx, r, phase, reason, report)
}
func (c *Reconciler) cleanup(ctx context.Context, r *ValidationRun) error {
	if !controllerutil.ContainsFinalizer(r, finalizer) {
		return nil
	}
	// Keep the allocation until all owned Jobs have disappeared.
	jobs := &batchv1.JobList{}
	if err := c.List(ctx, jobs, client.InNamespace(r.Namespace), client.MatchingLabels{"validation-run": r.Name, "app": "release-validation-controller"}); err != nil {
		return err
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !metav1.IsControlledBy(j, r) {
			continue
		}
		if err := c.Delete(ctx, j, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("waiting for job %s to be deleted", j.Name)
	}
	if err := c.release(ctx, r); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(r, finalizer)
	return c.Update(ctx, r)
}

// A ConfigMap update with resourceVersion is a compare-and-swap allocation.
func (c *Reconciler) allocate(ctx context.Context, r *ValidationRun) (bool, error) {
	allocated := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		err := c.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: allocationName}, cm)
		if apierrors.IsNotFound(err) {
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: allocationName, Namespace: c.Namespace}, Data: map[string]string{}}
			if err := c.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
			return apierrors.NewConflict(corev1.Resource("configmaps"), allocationName, errors.New("initializing allocation"))
		}
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		key := string(r.UID)
		if cm.Data[key] != "" {
			allocated = true
			return nil
		}
		count := 0
		for _, v := range cm.Data {
			if v == class(r) {
				count++
			}
		}
		limit := c.CPUCapacity
		if class(r) == "gpu-simulated" {
			limit = c.GPUCapacity
		}
		if count >= limit {
			allocated = false
			return nil
		}
		cm.Data[key] = class(r)
		if err := c.Update(ctx, cm); err != nil {
			return err
		}
		allocated = true
		return nil
	})
	return allocated, err
}
func (c *Reconciler) release(ctx context.Context, r *ValidationRun) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		err := c.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: allocationName}, cm)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, ok := cm.Data[string(r.UID)]; !ok {
			return nil
		}
		delete(cm.Data, string(r.UID))
		return c.Update(ctx, cm)
	})
}

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	ns := flag.String("namespace", "default", "namespace to watch")
	cpu := flag.Int("cpu-capacity", 2, "maximum concurrent CPU runs")
	gpu := flag.Int("gpu-simulated-capacity", 1, "maximum concurrent simulated GPU runs")
	flag.Parse()
	if *cpu < 1 || *gpu < 1 {
		fmt.Fprintln(os.Stderr, "capacities must be positive")
		os.Exit(2)
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	scheme.AddKnownTypes(groupVersion, &ValidationRun{}, &ValidationRunList{})
	metav1.AddToGroupVersion(scheme, groupVersion)
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{*ns: {}}}, LeaderElection: true, LeaderElectionID: "release-validation.learning.example.io", LeaderElectionNamespace: *ns})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	reconciler := &Reconciler{Client: mgr.GetClient(), Scheme: scheme, Namespace: *ns, CPUCapacity: *cpu, GPUCapacity: *gpu}
	if err := ctrl.NewControllerManagedBy(mgr).For(&ValidationRun{}).Owns(&batchv1.Job{}).Complete(reconciler); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.FromContext(context.Background()).Info("starting release validation controller", "namespace", *ns, "cpu", strconv.Itoa(*cpu), "gpu-simulated", strconv.Itoa(*gpu))
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
