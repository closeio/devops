Kops Runner
===

This utility is very specific to our k8s implementation.  The odds are high you would have to modify parts of it
to have it meet your needs.  It uses [kops](https://github.com/kubernetes/kops), boto3, and
[pykube](https://github.com/kelproject/pykube) to fully provision a Kubernetes cluster as defined in its configuration file.  

*Goals*
* Supplement Kops with functionality it doesn't currently have or is specific to our implementation.
* Support immutable infrastructure so clusters will be deleted and created regularly.  You cannot use Kops to manage the cluster
after creating it with `manage_cluster.py`.  Make manual/scripted changes or create a new cluster.
* Provide a process friendly to staff not familiar with k8s/kops to fully provision a cluster.  There are a number of checks to protect damage to running infrastructure.  These may get in the way.  Comment them out if you don't need them.
* Create the k8s cluster in private subnets but use public subnets to allow Internet facing ELBs to access services in the cluster.
The result will look something like [this](K8s-priv-pub-network.png).
* Manage security groups for instance groups.


Prerequisites
---

1. Python modules - `pip install -r requirements.txt`
1. `kops` binary on path
1. AWS credentials available and default region specified.  Something like `aws ec2 describe-instances` should work with
no issues.
1. Public subnets created and labeled appropriately.
1. Extra security group(s) created to be assigned to IGs.
1. The script needs access to the cluster.  So you need to run it from a node in AWS or use a bastion to tunnel from your
local workstation into AWS.  The script will wait until all nodes have registered in the cluster before it completes
its configuration.


Usage
---

**Configure:** The `k8s_clusters.yaml` file needs to be updated with your specific environment.

**Create:** `python manage_cluster.py create -e test -c k8s-a.example.com -f k8s_clusters.yaml`

The script does not complete until all nodes have registered.  You need to make sure it has a communication path to
the api server(s) so it can poll the status of the cluster.

**Delete:** `python manage_cluster.py delete -e test -c k8s-a.example.com -f k8s_clusters.yaml`

The script will check for running pods and services before it will allow you to delete a cluster.  If the cluster
did not get created completely this check will fail.  So you can force deletion with the `-D` argument.

**Force delete:** `python manage_cluster.py delete -e test -c k8s-a.example.com -f k8s_clusters.yaml -D`

It should be safe to repeatedly run the create and force delete command as you work out the settings in the configuration
file and AWS infrastructure.  You should get error messages that indicate the reason for a failure.

Those interested in looking at what the script does would probably want to jump to the `create_cluster_command` method
and start walking through each step there.
