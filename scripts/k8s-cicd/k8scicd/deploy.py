#!/usr/bin/python
"""Send deployment to k8s and wait for finish."""

import datetime
import logging
import sys
import time
import yaml

import jinja2
import pykube

def init():
    """Initialize system."""

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

def render_k8s_resource(file_name, variables):
    """Render k8s resource files using jinga2.

    Args:
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Rendered resource dict
    """

    with open(file_name, 'r') as deploy_file:
        deploy_string = deploy_file.read()

    deploy_template = jinja2.Template(deploy_string)
    deploy_template.environment.undefined = jinja2.StrictUndefined
    deploy_string = deploy_template.render(variables)

    return yaml.load(deploy_string)

def check_namespace(api, name):
    """Create namespace if it doesn't exist."""

    logging.info('Checking namespace %s', name)

    namespaces = pykube.Namespace.objects(api)

    for namespace in namespaces:
        if str(namespace) == str(name):
            return
    logging.info('Creating namespace')

    namespace_yaml = """
kind: Namespace
apiVersion: v1
metadata:
  name: %s
  labels:
    name: %s
""" % (name, name)

    namespace = pykube.Namespace(api, yaml.load(namespace_yaml))
    namespace.create()


def deploy_config_map(api, manifest, version, timeout):
    """Sends service to k8s cluster.

    First checks to see if deployment exists in cluster.  If it does not the deployment is created otherise
    the deployment is updated.

    Args:
        api: Pykube api instance
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Pykube deployment instance
    """

    logging.info("Deploying")

    configmap = pykube.ConfigMap(api, manifest)

    configmap.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in configmap.obj and 'namespace' in configmap.obj['metadata']:
        check_namespace(api, configmap.obj['metadata']['namespace'])

    if not configmap.exists():
        logging.info("Creating ConfigMap")
        configmap.create()
    else:
        logging.info("Updating ConfigMap")
        configmap.update()

    return configmap


def deploy_daemon_set(api, manifest, version):
    """Sends service to k8s cluster.

    First checks to see if deployment exists in cluster.  If it does not the deployment is created otherise
    the deployment is updated.

    Args:
        api: Pykube api instance
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Pykube deployment instance
    """

    logging.info("Deploying")

    daemon_set = pykube.DaemonSet(api, manifest)

    daemon_set.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in daemon_set.obj and 'namespace' in daemon_set.obj['metadata']:
        check_namespace(api, daemon_set.obj['metadata']['namespace'])

    if not daemon_set.exists():
        logging.info("Creating service")
        daemon_set.create()
    else:
        logging.info("Updating service")
        daemon_set.update()

    return daemon_set


def deploy_deployment(api, manifest, version, timeout):
    """Sends deployment to k8s cluster.

    First checks to see if deployment exists in cluster.  If it does not the deployment is created otherise
    the deployment is updated.

    Args:
        api: Pykube api instance
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Pykube deployment instance
    """

    logging.info("Deploying")

    deployment = pykube.Deployment(api, manifest)

    deployment.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in deployment.obj and 'namespace' in deployment.obj['metadata']:
        check_namespace(api, deployment.obj['metadata']['namespace'])

    if not deployment.exists():
        logging.info("Creating deployment")
        deployment.create()
    else:
        logging.info("Updating deployment")
        deployment.update()

    app_label = deployment.obj['metadata']['labels']['app']

    if 'metadata' in deployment.obj and 'namespace' in deployment.obj['metadata']:
        namespace = deployment.obj['metadata']['namespace']
    else:
        namespace = 'default'

    revision = get_revision(api, app_label, version, timeout, namespace)
    time.sleep(3)  # Hack to make sure deployment has a chance to start - Need a better way to detect this
    wait_for_deployment(deployment, revision, timeout)

    return deployment

def deploy_service(api, manifest, version, timeout):
    """Sends service to k8s cluster.

    First checks to see if deployment exists in cluster.  If it does not the deployment is created otherise
    the deployment is updated.

    Args:
        api: Pykube api instance
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Pykube deployment instance
    """

    logging.info("Deploying")

    service = pykube.Service(api, manifest)

    service.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in service.obj and 'namespace' in service.obj['metadata']:
        check_namespace(api, service.obj['metadata']['namespace'])

    if not service.exists():
        logging.info("Creating service")
        service.create()
    else:
        logging.info("Updating service")
        service.update()

    return service


def deploy_service_account(api, manifest, version):
    """Sends service to k8s cluster.

    First checks to see if deployment exists in cluster.  If it does not the deployment is created otherise
    the deployment is updated.

    Args:
        api: Pykube api instance
        file_name: A filename string for the yaml template
        version: Version string will be used as the jinja version variable
        tag: Image tag string will be used as a jinga imagetag variable

    Returns:
        Pykube deployment instance
    """

    logging.info("Deploying")

    service_account = pykube.ServiceAccount(api, manifest)

    service_account.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in service_account.obj and 'namespace' in service_account.obj['metadata']:
        check_namespace(api, service_account.obj['metadata']['namespace'])

    if not service_account.exists():
        logging.info("Creating service_account")
        service_account.create()
    else:
        logging.info("Updating service_account")
        service_account.update()

    return service_account


def get_revision(api, app_label, version, timeout=60, namespace="default"):
    """Polls k8s cluster to get deployment revision number.

    Using the app label for a ReplicaSet the cluster is polled to find the matching version label
    for this deployment.

    Args:
        api: Pykube api instance
        app_label: String representing the app name stored in the k8s 'app' label
        version: Version string used to label deploymen
        timeout: Seconds to poll looking for matching ReplicaSet

    Returns:
        Revision string

    Raises:
        RuntimeError: Raises exception if timeout is exceeded.
    """

    logging.info("Getting revision of our deployment: %s", app_label)
    start_time = datetime.datetime.now()
    while (datetime.datetime.now() - start_time).total_seconds() < timeout:
        replication_sets = pykube.ReplicaSet.objects(api).filter(
            selector={"app__in": {app_label}}).filter(namespace=namespace)
        for replica_set in replication_sets:
            if replica_set.obj['metadata']['labels']['version'] == version:
                logging.info("Our revision: %s", replica_set.annotations['deployment.kubernetes.io/revision'])
                return replica_set.annotations['deployment.kubernetes.io/revision']

        time.sleep(2)
    raise RuntimeError('Timeout')

def wait_for_deployment(deployment, our_revision, timeout=60):
    """Wait for deployment to complete.

    Polls k8s cluster waiting for the number of replicas to stabilize indicating a successful deployment.  Watches
    the revision number to determine if a new deployment cancelled this deployment.

    Args:
        deployment: Pykube deployment instance
        our_revision: This deployment's deployment string
        timeout: Seconds to poll cluster waiting for deployment to complete

    Returns:
        revision: String of the actual revision that got deployment.  May not be this deployment's revision.

    Raises:
        RuntimeError: Raises exception if timeout is exceeded.
    """

    logging.info("Waiting for deployment to finish")
    start_time = datetime.datetime.now()
    while (datetime.datetime.now() - start_time).total_seconds() < timeout:
        try:
            deployment.reload()
            current_revision = deployment.annotations['deployment.kubernetes.io/revision']
            logging.info("%s %s", deployment.obj['status'], current_revision)

            if int(current_revision) < int(our_revision):
                logging.info("Waiting for our deployment to start")
                time.sleep(2)
                continue

            if 'availableReplicas' in deployment.obj['status'] and 'updatedReplicas' in deployment.obj['status'] and \
                deployment.obj['status']['updatedReplicas'] == deployment.obj['status']['availableReplicas'] and \
                deployment.obj['status']['replicas'] == deployment.obj['status']['availableReplicas']:

                #Final check, just in case
                if 'unavailableReplicas' in deployment.obj['status'] and \
                    deployment.obj['status']['unavailableReplicas'] != 0:
                    logging.info("Unavailable != 0")
                    time.sleep(2)
                    continue

                logging.info("Done waiting for deployment")
                if current_revision != our_revision:
                    logging.info("Looks like our deployment got bumped ours: %s current: %s", \
                                 our_revision, current_revision)
                return current_revision

        except Exception:  #pylint: disable=w0703
            logging.error("Error:", exc_info=True)

        time.sleep(2)

    raise RuntimeError('Timeout')


def k8s_deploy_from_file(kube_config, manifest_filename, version, variables, timeout=240):
    """Deploy to cluster"""

    logging.info('Loading manifest')

    deploy_resource = render_k8s_resource(manifest_filename, variables)
    k8s_deploy_from_manifest(kube_config, deploy_resource, version, timeout)


def k8s_deploy_from_manifest(kube_config, manifest, version, timeout=240):
    """Deploy to cluster"""

    start_deployment = time.time()

    logging.info('Starting k8s deployment')

    api = pykube.HTTPClient(pykube.KubeConfig.from_file(kube_config))

    kind = manifest['kind']

    if kind == 'Deployment':
        deploy_deployment(api, manifest, version, timeout)
    elif kind == 'Service':
        deploy_service(api, manifest, version, timeout)
    elif kind == 'ConfigMap':
        deploy_config_map(api, manifest, version, timeout)
    elif kind == 'ServiceAccount':
        deploy_service_account(api, manifest, version)
    elif kind == 'DaemonSet':
        deploy_daemon_set(api, manifest, version)
    else:
        raise RuntimeError('Unsupported manifest kind')

    end_deployment = time.time()
    logging.info("Deploying complete. %ds", end_deployment-start_deployment)
