#!/usr/bin/python
"""Send deployment to k8s and wait for finish."""

import datetime
import logging
import time

import jinja2

import pykube

import yaml


class K8sDeployer(object):
    """Kubernetes Deployer."""

    def __init__(self, fast_mode=False):
        """Initialize system."""

        self.fast_mode = fast_mode
        self.api = None

    def _render_k8s_resource(self, deploy_string, variables, debug):
        """Render k8s resource files using jinga2.

        Args:
            file_name: A filename string for the yaml template
            version: Version string will be used as the jinja version variable
            tag: Image tag string will be used as a jinga imagetag variable

        Returns:
            Rendered resource dict

        """

        deploy_template = jinja2.Template(deploy_string)
        deploy_template.environment.undefined = jinja2.StrictUndefined
        deploy_string = deploy_template.render(variables)

        if debug:
            logging.info('\n{}'.format(deploy_string))

        return yaml.load(deploy_string)

    def _check_namespace(self, name):
        """Create namespace if it doesn't exist."""

        logging.info('Checking namespace %s', name)

        namespaces = pykube.Namespace.objects(self.api)

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

        namespace = pykube.Namespace(self.api, yaml.load(namespace_yaml))
        namespace.create()

    def _delete_pods(self, namespace, app, timeout):
        """Delete replica sets."""

        pods = pykube.Pod.objects(self.api).filter(
            namespace=namespace,
            selector={'app__in': [app]})

        for pod in pods:
            logging.info('Deleting pod: %s', pod.name)
            pod.delete()
            self._wait_for_object_removal(pod, timeout)

    def _delete_replica_sets(self, api_version, namespace, app, timeout):
        """Delete replica sets."""

        object_class = pykube.object_factory(self.api, api_version, 'ReplicaSet')
        replica_sets = object_class.objects(self.api).filter(
            namespace=namespace,
            selector={'app__in': [app]})
        for rs in replica_sets:
            logging.info('Deleting rs: %s', rs.name)
            rs.delete()
            self._wait_for_object_removal(rs, timeout)
            self._delete_pods(namespace, app, timeout)

    def _deploy_deployment(self, manifest, version, update, timeout):
        """Deploy Deployment."""

        deployment, updated = self._deploy_generic_manifest(manifest,
                                                            version, update,
                                                            timeout)

        if not updated:
            return

        # We wait for deployments finish
        app_label = deployment.obj['metadata']['labels']['app']

        if 'metadata' in deployment.obj and 'namespace' in deployment.obj['metadata']:
            namespace = deployment.obj['metadata']['namespace']
        else:
            namespace = 'default'

        revision = self._get_revision(app_label, version, timeout, namespace)

        self._wait_for_deployment(deployment, revision, timeout)

        return deployment

    def _deploy_generic_manifest(self, manifest, version, update, timeout):
        """Deploy generic manifest."""

        kind = manifest['kind']
        logging.info('Deploying manifest')

        object_class = pykube.object_factory(self.api, manifest['apiVersion'],
                                             manifest['kind'])

        k8s_object = object_class(self.api, manifest)

        k8s_object.annotations['kubernetes.io/change-cause'] = 'Deploying version %s' % version

        if 'metadata' in k8s_object.obj and 'namespace' in k8s_object.obj['metadata']:
            self._check_namespace(k8s_object.obj['metadata']['namespace'])

        updated = True
        if not k8s_object.exists():
            logging.info('Creating %s' % kind)
            k8s_object.create()
        elif update:
            logging.info('Updating %s' % kind)
            k8s_object.update()
        else:
            logging.info('Not updating %s' % kind)
            updated = False

        return k8s_object, updated

    def _deploy_statefulset(self, manifest, version, update, timeout):
        """Deploy Deployment."""

        statefulset, updated = self._deploy_generic_manifest(manifest,
                                                             version, update,
                                                             timeout)

        if not updated:
            return

        self._wait_for_statefulset(statefulset, timeout)

        return statefulset

    def _get_revision(self, app_label, version, timeout=60, namespace='default'):
        """Poll k8s cluster to get deployment revision number.

        Using the app label for a ReplicaSet the cluster is polled to find the
        matching version label for this deployment.

        Args:
            app_label: String representing the app name stored in the k8s 'app' label
            version: Version string used to label deploymen
            timeout: Seconds to poll looking for matching ReplicaSet

        Returns:
            Revision string

        Raises:
            RuntimeError: Raises exception if timeout is exceeded.

        """

        logging.info('Getting revision of our deployment: %s', app_label)
        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() < timeout:
            replication_sets = pykube.ReplicaSet.objects(self.api).filter(
                selector={'app__in': {app_label}}).filter(namespace=namespace)
            for replica_set in replication_sets:
                if 'version' in replica_set.obj['metadata']['annotations']:
                    rs_version = replica_set.obj['metadata']['annotations']['version']
                else:
                    # Support older deployments where the version is a label
                    rs_version = replica_set.obj['metadata']['labels']['version']
                if rs_version == version:
                    logging.info('Our revision: %s',
                                 replica_set.annotations['deployment.kubernetes.io/revision'])
                    return replica_set.annotations['deployment.kubernetes.io/revision']

            time.sleep(2)
        raise RuntimeError('Timeout')

    def _get_status(self, status_dict):
        status = '  '
        if 'replicas' in status_dict:
            status += 'Replicas:{} '.format(status_dict['replicas'])
        if 'availableReplicas' in status_dict:
            status += 'Available:{} '.format(status_dict['availableReplicas'])
        if 'unavailableReplicas' in status_dict:
            status += 'Unavailable:{} '.format(status_dict['unavailableReplicas'])
        if 'updatedReplicas' in status_dict:
            status += 'Updated:{} '.format(status_dict['updatedReplicas'])

        return status

    def _is_statefulset_updating(self, statefulset):
        if 'currentReplicas' in statefulset.obj['status'] and \
                statefulset.obj['spec']['replicas'] != statefulset.obj['status']['currentReplicas']:
            return True

        if 'updateRevision' in statefulset.obj['status'] \
                and statefulset.obj['status']['updateRevision'] != \
                statefulset.obj['status']['currentRevision']:
            return True
        else:
            return False

    def _undeploy_manifest(self, manifest, version, update, timeout):
        """Delete k8s object."""

        logging.info('Deleting k8s object')

        object_class = pykube.object_factory(self.api, manifest['apiVersion'],
                                             manifest['kind'])
        k8s_object = object_class(self.api, manifest)

        if k8s_object.exists():
            logging.info('Found object, deleting: %s', k8s_object.name)
            k8s_object.delete()
            self._wait_for_object_removal(k8s_object, timeout)
            if manifest['kind'] == 'Deployment':
                logging.info('Object is Deployment, cascading delete of ReplicaSets')
                self._delete_replica_sets(manifest['apiVersion'],
                                          k8s_object.obj['metadata']['namespace'],
                                          k8s_object.obj['metadata']['labels']['app'],
                                          timeout)
            elif manifest['kind'] == 'StatefulSet':
                logging.info('Object is StatefulSet, cascading delete of Pods')
                self._delete_pods(k8s_object.obj['metadata']['namespace'],
                                  k8s_object.obj['metadata']['labels']['app'],
                                  timeout)
        else:
            logging.info('Object not found')
            return

    def _wait_for_deployment(self, deployment, our_revision, timeout=60):
        """
        Wait for deployment to complete.

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

        if self.fast_mode:
            logging.info('Fast mode, not waiting')
            return
        else:
            logging.info('Waiting for deployment to finish')

        # Hack to make sure deployment has a chance to start - Need a better way to detect this
        time.sleep(3)

        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() < timeout:
            try:
                deployment.reload()
                current_revision = deployment.annotations['deployment.kubernetes.io/revision']
                status = self._get_status(deployment.obj['status'])
                logging.info('%sGeneration:%s', status, current_revision)

                if int(current_revision) < int(our_revision):
                    logging.info('Waiting for our deployment to start')
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
                        logging.info('Unavailable != 0')
                        time.sleep(2)
                        continue

                    logging.info('Done waiting for deployment')
                    if current_revision != our_revision:
                        logging.info('Looks like our deployment got bumped ours: %s current: %s',
                                     our_revision, current_revision)

                    # All checks passed so deployment looks successful
                    return current_revision

            except Exception:  # pylint: disable=w0703
                logging.error('Error:', exc_info=True)

            time.sleep(2)

        raise RuntimeError('Timeout')

    def _wait_for_object_removal(self, k8s_object, timeout):
        """Poll k8s waiting for object to be removed."""

        if self.fast_mode:
            return

        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() < timeout:
            logging.info('  Waiting for object to be removed')
            exists = k8s_object.exists()
            if not exists:
                return
            time.sleep(1)
        raise RuntimeError('Timeout')

    def _wait_for_statefulset(self, statefulset, timeout):
        if self.fast_mode:
            return

        # Wait 8 seconds so it detects the update.  Ideally this could
        # be changed to wait until it detects the update has started.
        time.sleep(8)

        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() < timeout:
            statefulset.reload()
            if self._is_statefulset_updating(statefulset):

                updated = statefulset.obj['status'].get('updatedReplicas', '-')
                replicas = statefulset.obj['status'].get('replicas', '-')
                desired = statefulset.obj['spec'].get('replicas', '-')
                if updated != '-' and desired != '-':
                    percent = '%.1f%%' % (float(updated) / desired * 100.0)
                else:
                    percent = '-'

                logging.info('  Waiting for StatefulSet desired: {} updated:'
                             ' {} replicas: {} complete: {}'
                             .format(desired, updated, replicas, percent))
                time.sleep(4)
                continue
            else:
                return

        raise RuntimeError('Timeout')

    def k8s_deploy_from_file(self, kube_config, manifest_filename, version, variables,
                             timeout=240, update=True, context=None, undeploy=False,
                             debug=False):
        """Deploy to cluster from a manifest file."""

        logging.info('Loading manifest %s', manifest_filename)

        with open(manifest_filename, 'r') as deploy_file:
            deploy_resource = deploy_file.read()
        self.k8s_deploy_from_manifest(kube_config, deploy_resource, version, variables, timeout,
                                      update, context, undeploy=undeploy, debug=debug)

    def k8s_deploy_from_manifest(self, kube_config, manifest, version, variables, timeout=240,
                                 update=True, context=None, undeploy=False,
                                 debug=False):
        """Deploy to cluster using provided manifest."""

        start_deployment = time.time()

        manifest = self._render_k8s_resource(manifest, variables, debug)

        logging.info('Starting k8s deployment')

        kubeconfig = pykube.KubeConfig.from_file(kube_config)
        if context:
            logging.info('Setting kube context: %s', context)
            kubeconfig.set_current_context(context)

        self.api = pykube.HTTPClient(kubeconfig)

        kind = manifest['kind']

        if undeploy:
            self._undeploy_manifest(manifest, version, update, timeout)
        else:
            if kind == 'Deployment':
                self._deploy_deployment(manifest, version, update, timeout)
            if kind == 'StatefulSet':
                self._deploy_statefulset(manifest, version, update, timeout)
            else:
                self._deploy_generic_manifest(manifest, version, update, timeout)

        end_deployment = time.time()
        logging.info('Deploying complete. %ds', end_deployment - start_deployment)
