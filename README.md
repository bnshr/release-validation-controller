# Release validation controller (learning project)

This small controller accepts `ValidationRun` resources, starts one Kubernetes Job per attempt, and writes a qualification decision and reason to `.status.report`. It is an orchestration exercise, not a release certification system.

## Set up on macOS

Install [Homebrew](https://brew.sh), then run from this directory. The script installs missing command line tools with Homebrew and starts Colima if Docker is unavailable:

```sh
bash scripts/setup-kind-macos.sh
```

The script creates or reuses the `release-validation` kind cluster, builds and loads the controller image, installs the CRD and RBAC, restarts the Deployment to pick up the image, and verifies the example run. Every `kubectl` command uses the explicit `kind-release-validation` context. Run it again after changing the controller.

For a retry and failure check, create a new run with suite `fail`, `retries: 1`, and a unique name; expect `.status.phase` to become `Failed` and `.status.attempt` to become `2`. For cancellation, create a run and set `spec.cancel: true`; expect `Cancelled`. To observe queuing, set the controller's `--cpu-capacity=1` and create several runs together. To check restart recovery, restart the Deployment during a run with `kubectl --context kind-release-validation rollout restart deployment/release-validation-controller`.

Inspect progress with `kubectl --context kind-release-validation get validationruns -w`, controller logs with `kubectl --context kind-release-validation logs deployment/release-validation-controller`, and Jobs with `kubectl --context kind-release-validation get jobs`. Completed Jobs are deleted after the terminal result is recorded. Tear down the disposable cluster with `kind delete cluster --name release-validation`.

The manifest assumes namespace `default`. The controller also runs out of cluster with `go run . --namespace=default` using your kubeconfig after applying the CRD and RBAC. The Job uses BusyBox and a scripted result: `smoke`, `network`, `storage`, and `gpu-smoke` pass; `fail` fails. Set `spec.cancel: true` to cancel. `timeoutSeconds` applies from the first admitted attempt. `retries` counts additional attempts.

Capacity is tracked in a ConfigMap using resource version updates. CPU and simulated GPU slots have separate limits. The finalizer deletes owned Jobs and releases a slot when a run is deleted or reaches a terminal phase; the run's status and named Jobs let reconciliation resume after controller restarts. A queued run is retried every five seconds.

## Failure behavior

- Restart or duplicate event: reconciliation reads the current `ValidationRun`, allocation ConfigMap, and deterministic Job name. An existing allocation or Job is reused, so another Job is not created for the same attempt.
- Failed API call: reconciliation returns the error and controller-runtime retries it. ConfigMap resource-version conflicts are retried within the allocation operation. A failed status update after Job creation leaves that Job discoverable on the next pass.
- Partial completion: the terminal phase is saved before Job cleanup. Cleanup deletes owned Jobs, waits for them to disappear, releases the capacity key, and removes the finalizer. A failure at any step leaves the finalizer in place so a later pass can resume.
- Job watch events trigger reconciliation; periodic requeues cover queued and running runs. The controller owns only Jobs whose controller reference points to that run, even if another Job uses the expected name.

Run `go test ./...` for injected status failure, duplicate event, restart, and interrupted cleanup checks.

## Limits

- `gpu-simulated` consumes only a logical slot; it neither schedules a GPU nor tests GPU hardware.
- The scripts test orchestration behavior only, not a real release artifact or cluster qualification.
- A run deleted while the controller is stopped can leave an allocation behind; remove its UID key from the `validation-capacity` ConfigMap after verifying its Jobs are gone.
- Kubernetes foreground Job deletion is asynchronous; capacity is retained until the Job object disappears. Pods with finalizers or other deletion delays can keep a slot occupied.
- Run specs should be treated as immutable after creation, apart from `cancel`. This example has no admission webhook for that rule.
- No fairness guarantee is made between queued runs. This example targets one namespace and uses a leader election lease in `default`.
