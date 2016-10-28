Kubernetes CI/CD
===

These scripts facilitate building, testing, and deploying Docker images to Kubernetes clusters.  Each step is defined
in the `service.yaml` file.  The `k8scicd.sh` script can then be used to run through each CI/CD phase.  This typically
includes building, testing, pushing to a repository, deploying the the k8s cluster and then pruning old images from the
docker repository.

Variables
---
Key to this process is using Jinja2 templates to control various parameters for each phase.  Two important built-in
variables (can be overridden) are the `KUBE_CONFIG` and `VERSION` variables.  The `KUBE_CONFIG` needs to point to a
valid kubectl config file or the default of `~/.kube/config` will be used.  The `VERSION` variable is important to
ensure artifacts are tagged with the appropriate version during each phase.  If not specified on the command line it
will default to the current timestamp.
