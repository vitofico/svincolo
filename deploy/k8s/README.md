# Isolated Kubernetes canary

`canary.yaml` creates a single replica with a read-only root filesystem,
an internal Service and default-deny network policy. Import the local image
as `svincolo:canary` into the target node's Kubernetes container runtime.
The manifest uses the imported image.

Create the `svincolo` namespace and provide a Secret called
`svincolo-config` in it. Include `svincolo.toml`, the caller token file
and any upstream key files referenced by the config. Use a file path relative
to the config or an absolute path under `/run/svincolo`. Keep this Secret in
the cluster's private provisioning path.

Mount the whole Secret directory as in this manifest so Kubernetes can
project updated config and keys. The router checks for changes every ten
seconds after the updated volume reaches the pod. A rejected update leaves
the previous configuration serving. Keep coordinated changes in the same
Secret so the volume projects them as one generation.

Set the server listen address to `0.0.0.0:8080` and configure at least one
caller. Set `drain_delay_s` plus `grace_s` below the pod's 40-second
termination period. Start with `max_in_flight = 8` and
`max_buffered_bytes = 33554432` for the canary's memory limit.

The example uses HTTP readiness on the shared listener. At the connection
limit, a new probe connection can be refused and the pod can become unready.
For an existing deployment, remove HTTP liveness on that listener before
enabling the connection cap so temporary saturation cannot trigger a restart.

Apply the manifest and wait for readiness.

```sh
kubectl apply -f deploy/k8s/canary.yaml
kubectl -n svincolo rollout status deployment/svincolo
kubectl -n svincolo port-forward service/svincolo 18080:8080
```

Use the forwarded port for health and caller-authentication checks. Add
cluster-specific allow policies for DNS and the selected upstream before
testing a model request. Keep those policies with the deployment configuration.
