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


def deploy_cluster_role(api, manifest, version, update):
    """Deploy Service Account."""

    logging.info("Deploying cluster role")

    role = pykube.ClusterRole(api, manifest)

    role.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in role.obj and 'namespace' in role.obj['metadata']:
        check_namespace(api, role.obj['metadata']['namespace'])

    if not role.exists():
        logging.info("Creating ClusterRole")
        role.create()
    elif update:
        logging.info("Updating ClusterRole")
        role.update()
    else:
        logging.info('Not updating ClusterRole')

    return role


def deploy_cluster_role_binding(api, manifest, version, update):
    """Deploy Service Account."""

    logging.info("Deploying ClusterRoleBinding")

    role_binding = pykube.ClusterRoleBinding(api, manifest)

    role_binding.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in role_binding.obj and 'namespace' in role_binding.obj['metadata']:
        check_namespace(api, role_binding.obj['metadata']['namespace'])

    if not role_binding.exists():
        logging.info("Creating ClusterRoleBinding")
        role_binding.create()
    elif update:
        logging.info("Updating ClusterRoleBinding")
        role_binding.update()
    else:
        logging.info('Not updating ClusterRoleBinding')

    return role_binding


def deploy_config_map(api, manifest, version, timeout, update):
    """Deploy Config Map."""

    logging.info("Deploying configmap")

    configmap = pykube.ConfigMap(api, manifest)

    configmap.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in configmap.obj and 'namespace' in configmap.obj['metadata']:
        check_namespace(api, configmap.obj['metadata']['namespace'])

    if not configmap.exists():
        logging.info("Creating ConfigMap")
        configmap.create()
    elif update:
        logging.info("Updating ConfigMap")
        configmap.update()
    else:
        logging.info('Not updating ConfigMap')

    return configmap


def deploy_daemon_set(api, manifest, version, update):
    """Deploy Daemon Set."""

    logging.info("Deploying daemonset")

    daemon_set = pykube.DaemonSet(api, manifest)

    daemon_set.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in daemon_set.obj and 'namespace' in daemon_set.obj['metadata']:
        check_namespace(api, daemon_set.obj['metadata']['namespace'])

    if not daemon_set.exists():
        logging.info("Creating DaemonSet")
        daemon_set.create()
    elif update:
        logging.info("Updating DaemonSet")
        daemon_set.update()
    else:
        logging.info('Not updating DaemonSet')

    return daemon_set


def deploy_deployment(api, manifest, version, timeout, update):
    """Deploy Deployment."""

    logging.info("Deploying deployment")

    deployment = pykube.Deployment(api, manifest)

    deployment.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in deployment.obj and 'namespace' in deployment.obj['metadata']:
        check_namespace(api, deployment.obj['metadata']['namespace'])

    if not deployment.exists():
        logging.info('Creating deployment')
        deployment.create()
    elif update:
        logging.info('Updating deployment')
        deployment.update()
    else:
        logging.info('Not updating deployment')
        return

    # We wait for deployments finish
    app_label = deployment.obj['metadata']['labels']['app']

    if 'metadata' in deployment.obj and 'namespace' in deployment.obj['metadata']:
        namespace = deployment.obj['metadata']['namespace']
    else:
        namespace = 'default'

    revision = get_revision(api, app_label, version, timeout, namespace)
    # Hack to make sure deployment has a chance to start - Need a better way to detect this
    time.sleep(3)
    wait_for_deployment(deployment, revision, timeout)

    return deployment


def deploy_role(api, manifest, version, update):
    """Deploy Service Account."""

    logging.info("Deploying role")

    role = pykube.Role(api, manifest)

    role.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in role.obj and 'namespace' in role.obj['metadata']:
        check_namespace(api, role.obj['metadata']['namespace'])

    if not role.exists():
        logging.info("Creating Role")
        role.create()
    elif update:
        logging.info("Updating Role")
        role.update()
    else:
        logging.info('Not updating Role')

    return role


def deploy_role_binding(api, manifest, version, update):
    """Deploy Service Account."""

    logging.info("Deploying RoleBinding")

    role_binding = pykube.RoleBinding(api, manifest)

    role_binding.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in role_binding.obj and 'namespace' in role_binding.obj['metadata']:
        check_namespace(api, role_binding.obj['metadata']['namespace'])

    if not role_binding.exists():
        logging.info("Creating RoleBinding")
        role_binding.create()
    elif update:
        logging.info("Updating RoleBinding")
        role_binding.update()
    else:
        logging.info('Not updating RoleBinding')

    return role_binding


def deploy_service(api, manifest, version, timeout, update):
    """Deploy Service."""

    logging.info("Deploying service")

    service = pykube.Service(api, manifest)

    service.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in service.obj and 'namespace' in service.obj['metadata']:
        check_namespace(api, service.obj['metadata']['namespace'])

    if not service.exists():
        logging.info("Creating Service")
        service.create()
    elif update:
        logging.info("Updating Service")
        service.update()
    else:
        logging.info('Not updating Service')

    return service


def deploy_service_account(api, manifest, version, update):
    """Deploy Service Account."""

    logging.info("Deploying service account")

    service_account = pykube.ServiceAccount(api, manifest)

    service_account.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

    if 'metadata' in service_account.obj and 'namespace' in service_account.obj['metadata']:
        check_namespace(api, service_account.obj['metadata']['namespace'])

    if not service_account.exists():
        logging.info("Creating ServiceAccount")
        service_account.create()
    elif update:
        logging.info("Updating ServiceAccount")
        service_account.update()
    else:
        logging.info('Not updating ServiceAccount')

    return service_account


def get_revision(api, app_label, version, timeout=60, namespace="default"):
    """Poll k8s cluster to get deployment revision number.

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
                logging.info("Our revision: %s",
                             replica_set.annotations['deployment.kubernetes.io/revision'])
                return replica_set.annotations['deployment.kubernetes.io/revision']

        time.sleep(2)
    raise RuntimeError('Timeout')


def wait_for_deployment(deployment, our_revision, timeout=60):
    """Wait for deployment to complete.

    Polls k8s cluster waiting for the number of replicas to stabilize indicating
    a successful deployment.  Watches the revision number to determine if a new
    deployment cancelled this deployment.

    Args:
        deployment: Pykube deployment instance
        our_revision: This deployment's deployment string
        timeout: Seconds to poll cluster waiting for deployment to complete

    Returns:
        revision: String of the actual revision that got deployment.  May not be
                  this deployment's revision.

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

            if ('availableReplicas' in deployment.obj['status'] and
                    'updatedReplicas' in deployment.obj['status'] and
                    deployment.obj['status']['updatedReplicas'] ==
                    deployment.obj['status']['availableReplicas'] and
                    deployment.obj['status']['replicas'] ==
                    deployment.obj['status']['availableReplicas']):

                # Final check, just in case
                if ('unavailableReplicas' in deployment.obj['status'] and
                        deployment.obj['status']['unavailableReplicas'] != 0):
                    logging.info("Unavailable != 0")
                    time.sleep(2)
                    continue

                logging.info("Done waiting for deployment")
                if current_revision != our_revision:
                    logging.info("Looks like our deployment got bumped ours: %s current: %s",
                                 our_revision, current_revision)

                # All checks passed so deployment looks successful
                return current_revision

        except Exception:  # pylint: disable=w0703
            logging.error("Error:", exc_info=True)

        time.sleep(2)

    raise RuntimeError('Timeout')


def k8s_deploy_from_file(kube_config, manifest_filename, version, variables,
                         timeout=240, update=True, context=None):
    """Deploy to cluster from a manifest file"""

    logging.info('Loading manifest %s', manifest_filename)

    deploy_resource = render_k8s_resource(manifest_filename, variables)
    k8s_deploy_from_manifest(kube_config, deploy_resource, version, timeout, update, context)


def k8s_deploy_from_manifest(kube_config, manifest, version, timeout=240,
                             update=True, context=None):
    """Deploy to cluster using provided manifest"""

    start_deployment = time.time()

    logging.info('Starting k8s deployment')

    kubeconfig = pykube.KubeConfig.from_file(kube_config)
    if context:
        logging.info('Setting kube context: %s', context)
        kubeconfig.set_current_context(context)
    api = pykube.HTTPClient(kubeconfig)

    kind = manifest['kind']

    if kind == 'Deployment':
        deploy_deployment(api, manifest, version, timeout, update)
    elif kind == 'Service':
        deploy_service(api, manifest, version, timeout, update)
    elif kind == 'ConfigMap':
        deploy_config_map(api, manifest, version, timeout, update)
    elif kind == 'ServiceAccount':
        deploy_service_account(api, manifest, version, update)
    elif kind == 'DaemonSet':
        deploy_daemon_set(api, manifest, version, update)
    elif kind == 'Role':
        deploy_role(api, manifest, version, update)
    elif kind == 'RoleBinding':
        deploy_role_binding(api, manifest, version, update)
    elif kind == 'ClusterRole':
        deploy_cluster_role(api, manifest, version, update)
    elif kind == 'ClusterRoleBinding':
        deploy_cluster_role_binding(api, manifest, version, update)
    else:
        raise RuntimeError('Unsupported manifest kind')

    end_deployment = time.time()
    logging.info("Deploying complete. %ds", end_deployment - start_deployment)
