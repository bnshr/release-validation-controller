# Release validation controller (learning project)

This small controller accepts `ValidationRun` resources, starts one Kubernetes Job per attempt, and writes a qualification decision and reason to `.status.report`. It is an orchestration exercise, not a release certification system.

## Test in a disposable kind cluster

Start a local container runtime, then run these commands from this directory. The explicit `--context` prevents accidentally applying this example to another cluster.

```sh
kind create cluster --name release-validation --wait 2m
docker build -t release-validation-controller:local .
kind load docker-image release-validation-controller:local --name release-validation
kubectl --context kind-release-validation apply -f config/install.yaml
kubectl --context kind-release-validation rollout status deployment/release-validation-controller --timeout=2m
kubectl --context kind-release-validation apply -f config/example.yaml
kubectl --context kind-release-validation wait validationrun/example-qualification --for=jsonpath='{.status.phase}'=Passed --timeout=2m
kubectl --context kind-release-validation get validationrun example-qualification -o jsonpath='{.status.report}{"\n"}'
```

For a retry and failure check, create a new run with suite `fail`, `retries: 1`, and a unique name; expect `.status.phase` to become `Failed` and `.status.attempt` to become `2`. For cancellation, create a run and set `spec.cancel: true`; expect `Cancelled`. To observe queuing, set the controller's `--cpu-capacity=1` and create several runs together. To check restart recovery, restart the Deployment during a run with `kubectl --context kind-release-validation rollout restart deployment/release-validation-controller`.

Inspect progress with `kubectl --context kind-release-validation get validationruns -w`, controller logs with `kubectl --context kind-release-validation logs deployment/release-validation-controller`, and Jobs with `kubectl --context kind-release-validation get jobs`. Completed Jobs are deleted after their result is recorded. Tear down the disposable cluster with `kind delete cluster --name release-validation`.

The manifest assumes namespace `default`. The controller also runs out of cluster with `go run . --namespace=default` using your kubeconfig after applying the CRD and RBAC. The Job uses BusyBox and a scripted result: `smoke`, `network`, `storage`, and `gpu-smoke` pass; `fail` fails. Set `spec.cancel: true` to cancel. `timeoutSeconds` applies from the first admitted attempt. `retries` counts additional attempts.

Capacity is tracked in a ConfigMap using resource version updates. CPU and simulated GPU slots have separate limits. The finalizer releases a slot when a run is deleted; the run's status and named Jobs let reconciliation resume after controller restarts. A queued run is retried every five seconds.

## Limits

- `gpu-simulated` consumes only a logical slot; it neither schedules a GPU nor tests GPU hardware.
- The scripts test orchestration behavior only, not a real release artifact or cluster qualification.
- A run deleted while the controller is stopped can leave an allocation behind; remove its UID key from the `validation-capacity` ConfigMap after verifying its Jobs are gone.
- Job cleanup is asynchronous, so a slot can briefly be released while a cancelled or timed-out Pod is terminating.
- Run specs should be treated as immutable after creation, apart from `cancel`. This example has no admission webhook for that rule.
- No fairness guarantee is made between queued runs. This example targets one namespace and uses a leader election lease in `default`.
