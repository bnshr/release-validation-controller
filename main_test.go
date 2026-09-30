package main

import (
	"context"
	"errors"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type failingClient struct {
	client.Client
	failStatus bool
}

type failingStatusWriter struct {
	client.SubResourceWriter
	parent *failingClient
}

func (c *failingClient) Status() client.SubResourceWriter {
	return &failingStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}
func (w *failingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.parent.failStatus {
		w.parent.failStatus = false
		return errors.New("injected status failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func testReconciler(t *testing.T) (*Reconciler, *failingClient, types.NamespacedName) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	scheme.AddKnownTypes(groupVersion, &ValidationRun{}, &ValidationRunList{})
	run := &ValidationRun{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: types.UID("run-uid"), Finalizers: []string{finalizer}}, Spec: ValidationRunSpec{Version: "1", Suite: "fail", Retries: 1}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&ValidationRun{}, &batchv1.Job{}).WithObjects(run).Build()
	wrapped := &failingClient{Client: base}
	return &Reconciler{Client: wrapped, Scheme: scheme, Namespace: "default", CPUCapacity: 1, GPUCapacity: 1}, wrapped, types.NamespacedName{Namespace: "default", Name: "test"}
}

func reconcile(t *testing.T, c *Reconciler, name types.NamespacedName) error {
	t.Helper()
	_, err := c.Reconcile(context.Background(), ctrl.Request{NamespacedName: name})
	return err
}

func TestDuplicateAndRestartAfterStatusFailure(t *testing.T) {
	c, wrapped, name := testReconciler(t)
	wrapped.failStatus = true
	if err := reconcile(t, c, name); err == nil {
		t.Fatal("expected injected status failure")
	}
	if err := reconcile(t, c, name); err != nil {
		t.Fatal(err)
	}
	// A new reconciler represents a restarted process with only API state available.
	restarted := *c
	if err := reconcile(t, &restarted, name); err != nil {
		t.Fatal(err)
	}
	jobs := &batchv1.JobList{}
	if err := c.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one Job, got %d", len(jobs.Items))
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: allocationName}, cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.Data) != 1 {
		t.Fatalf("expected one allocation, got %v", cm.Data)
	}
}

func TestTerminalStatusSurvivesCleanupFailure(t *testing.T) {
	c, _, name := testReconciler(t)
	run := &ValidationRun{}
	if err := c.Get(context.Background(), name, run); err != nil {
		t.Fatal(err)
	}
	if _, err := c.allocate(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	job := c.makeJob(run)
	if err := c.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := c.finish(context.Background(), run, "Cancelled", "Cancellation requested"); err != nil {
		t.Fatal(err)
	}
	if err := reconcile(t, c, name); err == nil {
		t.Fatal("expected cleanup to wait for Job deletion")
	}
	if err := c.Get(context.Background(), name, run); err != nil {
		t.Fatal(err)
	}
	if run.Status.Phase != "Cancelled" {
		t.Fatalf("terminal phase lost: %s", run.Status.Phase)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: allocationName}, cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.Data) != 1 {
		t.Fatal("allocation released before Job disappeared")
	}
	if err := reconcile(t, c, name); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: allocationName}, cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.Data) != 0 {
		t.Fatal("allocation was not released")
	}
}
