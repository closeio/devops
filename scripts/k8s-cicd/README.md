Kubernetes CI/CD
===

This toolkit facilitates building, testing, and deploying Docker images to Kubernetes clusters.  Each step is defined
in the `service.yaml` file.  The `k8scicd.sh` script can then be used to run through each CI/CD phase.  This typically
includes building, testing, pushing to a repository, deploying the the k8s cluster and then pruning old images from a
docker repository.

Variables
---
Key to this process is using Jinja2 templates to control various parameters for each phase.  Two important built-in
variables (can be overridden) are the `KUBE_CONFIG` and `VERSION` variables.  The `KUBE_CONFIG` needs to point to a
valid kubectl config file or the default of `~/.kube/config` will be used.  The `VERSION` variable is important to
ensure artifacts are tagged with the appropriate version during each phase.  If not specified on the command line it
will default to the current timestamp.  Another variable set by the script is `SERVICE_DIRECTORY` which can be used to
find resource files.  The current directory is set to the service diretory before each command is run.


Example
---

The `service.yaml` file in the `examples/nginx` directory includes all supported command types (e.g. docker, script, etc).  
You could use the following command to run through all phases defined in the service file:
`./k8scicd.sh -p build,test,push,deploy,clean -d examples/ -s -v aws_ecr=111111111111.dkr.ecr.us-west-1.amazonaws.com -v aws_account=111111111111`

Note:

  * There are no predefined stages.  You can create whatever stages you need and specify with the -p argument.
  * The `-d examples/` argument will cause the script to process all subdirectories in `examples/`.  In this case its a single
  directory but in more complicated configurations there could be multiple services and the order each service is processed
  can be controlled with the `config: order` setting.  This is important if you need to build a Docker image first and then
  use it in a subsequent k8s deployment.
  * You can also specify `-d examples/nginx` and then it would only process that single directory because it would find
  a `service.yaml` file.
  * By default `k8scicd.py` looks for `service.yaml` files but this can be changed by specifying the `-f` argument.
