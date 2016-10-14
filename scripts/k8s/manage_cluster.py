"""Manage k8s Clusters."""

import argparse
import base64
import logging
import os
import subprocess
import sys
import time
import urlparse
import yaml

import boto3
import jinja2
import pykube
import requests

def init():
    """Initialize system."""

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


class ManageCluster(object):
    """K8s cluster manager class."""

    def __init__(self):
        self.command = None
        self.environment = None
        self.cluster_name = None
        self.config = None
        self.config_filename = None
        self.cluster = None
        self.cluster_config = None
        self.kops_bin = 'kops'
        self.kube_config = os.path.realpath(os.path.expanduser('~/.kube/config'))
        self.force_delete = False
        self.args = None

    def _add_labels_to_instance_group(self, ig_name, instance_group):
        """Add labeling to instance group."""

        logging.info('Adding labeling to instance group %s', ig_name)
        s3_resource = boto3.resource('s3')
        ig_s3_object = s3_resource.Object(self.config['environments'][self.environment]['config']['s3_bucket'],
                                          self.cluster_name + '/instancegroup/%s' % ig_name)
        ig_config = yaml.load(ig_s3_object.get()['Body'].read())

        if 'labels' in instance_group and 'aws' in instance_group['labels']:
            labels = {}
            for label in instance_group['labels']['aws']:
                labels[label] = instance_group['labels']['aws'][label]
            ig_config['spec']['cloudLabels'] = labels

        if 'labels' in instance_group and 'k8s' in instance_group['labels']:
            labels = {}
            for label in instance_group['labels']['k8s']:
                labels[label] = instance_group['labels']['k8s'][label]
            ig_config['spec']['nodeLabels'] = labels

        # Update file in S3
        ig_s3_object.put(Body=yaml.safe_dump(ig_config, default_flow_style=False))

    def _check_for_cluster_in_s3(self):
        """Checks to see if a specifc s3 state folder exists."""

        logging.info('Checking S3 bucket for cluster config')
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(self.config['environments'][self.environment]['config']['s3_bucket'])
        objs = list(bucket.objects.filter(Prefix=self.cluster_name))
        if len(objs) > 0:
            logging.info("Cluster state folder already exists in S3")
            return True
        else:
            logging.info("Cluster state folder does not exist in S3")
            return False

    def _check_for_ec2_instances(self):
        """Find instance existence based on KubernetesCluster tag"""

        ec2 = boto3.resource('ec2')
        cluster_instances = ec2.instances.filter(Filters=
                                                 [{'Name': 'tag:KubernetesCluster', 'Values': [self.cluster_name]},
                                                  {'Name': 'instance-state-name', 'Values': ['pending', 'running']}])

        if len(list(cluster_instances.all())) > 0:
            logging.info("Cluster instances exist %d", len(list(cluster_instances.all())))
            return True
        else:
            logging.info('No cluster instances found')
            return False

    def _create_instance_group(self, name):
        """Create instance group by duplicating nodes ig."""

        logging.info('Creating instance group %s', name)

        s3_resource = boto3.resource('s3')
        ig_nodes_object = s3_resource.Object(self.config['environments'][self.environment]['config']['s3_bucket'],
                                             self.cluster_name + '/instancegroup/nodes')
        ig_nodes_config = yaml.load(ig_nodes_object.get()['Body'].read())

        instance_group = self.cluster['instance_groups'][name]
        # We clone the nodes IG config and turn it into a new IG with out settings
        ig_nodes_config['metadata']['name'] = name
        ig_nodes_config['spec']['maxSize'] = instance_group['number']
        ig_nodes_config['spec']['minSize'] = instance_group['number']
        ig_nodes_config['spec']['zones'] = instance_group['zones']
        ig_nodes_config['spec']['machineType'] = instance_group['type']

        if 'labels' in instance_group and 'aws' in instance_group['labels']:
            labels = {}
            for label in instance_group['labels']['aws']:
                labels[label] = instance_group['labels']['aws'][label]
            ig_nodes_config['spec']['cloudLabels'] = labels
        else:
            del ig_nodes_config['spec']['cloudLabels']

        if 'labels' in instance_group and 'k8s' in instance_group['labels']:
            labels = {}
            for label in instance_group['labels']['k8s']:
                labels[label] = instance_group['labels']['k8s'][label]
            ig_nodes_config['spec']['nodeLabels'] = labels
        else:
            del ig_nodes_config['spec']['nodeLabels']

        ig_new_ig_object = s3_resource.Object(self.config['environments'][self.environment]['config']['s3_bucket'],
                                              self.cluster_name + '/instancegroup/%s' % name)

        ig_new_ig_object.put(Body=yaml.safe_dump(ig_nodes_config, default_flow_style=False))

    def _create_instance_groups(self):
        """Create instance groups."""

        logging.info('Creating instance groups')
        for instance_group in self.cluster['instance_groups']:
            # The nodes instance group is created with the cluster, so we skip it here
            if instance_group == 'nodes':
                continue
            self._create_instance_group(instance_group)

    def _deploy_addon(self, url):
        """Deploy addon."""
            # Deploy dashboard addon
        addon_yaml = requests.get(url)

        # The above yaml file has the deployment and service manifest in same file, kubectl can handle that but
        # pykube can't
        addon = []
        for doc in addon_yaml.text.split('---'):
            addon.append(yaml.load(doc))

        api = pykube.HTTPClient(self._get_kops_config())

        for manifest in addon:
            if manifest['kind'] == 'Deployment':
                logging.info('Creating addon deployment')
                # Add node selector for utility instance group
                manifest['spec']['nodeSelector'] = {'close.io/ig': 'utility'}
                pykube.Deployment(api, manifest).create()
            elif manifest['kind'] == 'Service':
                logging.info('Creating addon service')
                pykube.Service(api, manifest).create()
            else:
                raise SystemError('Unkown manifest type')

    def _deploy_addons(self):
        """Deploy addons for to cluster."""

        logging.info('Deploying addons to new cluster')
        self._deploy_addon(
            'https://raw.githubusercontent.com/kubernetes/kops/master/addons/monitoring-standalone/v1.2.0.yaml')
        self._deploy_addon(
            'https://raw.githubusercontent.com/kubernetes/kops/v1.4.0/addons/kubernetes-dashboard/v1.4.0.yaml')

    def _fixup_cluster_config(self):
        """Fix the state object for things we can't pass to kops (yet)"""

        # The goal is to have this method empty by getting kops to support everything
        # we need.  But for now we fix it outselves.

        logging.info('Fixing up cluster config')

        s3_resource = boto3.resource('s3')
        cluster_s3_object = s3_resource.Object(self.config['environments'][self.environment]['config']['s3_bucket'],
                                               self.cluster_name + '/config')
        cluster_state_config = yaml.load(cluster_s3_object.get()['Body'].read())

        # Change internal k8s network to our subnet
        cluster_state_config['spec']['nonMasqueradeCIDR'] = self.cluster['subnets']['non_masq_subnet']

        # Set correct CIDR for each zone
        zones = []
        for zone in self.cluster['subnets']['zones']:
            zones.append({'name': zone, 'cidr': self.cluster['subnets']['zones'][zone]})
        cluster_state_config['spec']['zones'] = zones

        # Update file in S3
        cluster_s3_object.put(Body=yaml.safe_dump(cluster_state_config, default_flow_style=False))

        # Add AWS node and k8s node labeling configuration
        logging.info('Fixing up instance group configs')
        self._add_labels_to_instance_group('nodes', self.cluster['instance_groups']['nodes'])

        for zone in self.cluster['master']['zones']:
            self._add_labels_to_instance_group('master-%s' % zone, self.cluster['master'])

        # Add our secrets
        logging.info('Adding our secrets')
        s3_client = boto3.client('s3')
        for secret in self.cluster['secrets']:
            logging.info('Copying secret %s', secret)
            # This will copy it directly from our s3 secrets bucket to this cluster's state folder
            s3_client.copy_object(Bucket=self.cluster_config['s3_bucket'],
                                  Key='%s/secrets/%s' % (self.cluster_name, secret),
                                  CopySource={'Bucket': self.cluster['secrets'][secret]['bucket'],
                                              'Key': self.cluster['secrets'][secret]['key']})

        # Fix number of zones for node instance group
        # This defaults to all of the zones passed on the command
        # line but we might have a different setting in our config
        self._set_zones_for_instance_group('nodes')


    def _fixup_iam_policies(self):
        """Change policies to our more restrictive policies."""

        logging.info("Fixing up IAM policies")
        iam = boto3.resource('iam')


        role = iam.Role('masters.%s' % self.cluster_name)
        role.load()
        # We assume the policy files are in the same directory as the configuration file
        self._set_iam_policies(os.path.dirname(os.path.realpath(self.config_filename)) + '/' +
                               self.cluster['master_policy'], role, 'masters.%s' % self.cluster_name)

        role = iam.Role('nodes.%s' % self.cluster_name)
        role.load()
        self._set_iam_policies(os.path.dirname(os.path.realpath(self.config_filename)) + '/' +
                               self.cluster['node_policy'], role, 'nodes.%s' % self.cluster_name)

    def _fixup_infrastructure(self):
        """Fix infrastructure created by kops."""

        logging.info('Fixing infrastructure')
        ec2 = boto3.resource('ec2')
        vpc = ec2.Vpc(self.cluster_config['vpc_id'])
        vpc.load()

        # Switch default route from IGW to NGW.  This is the way we support private subnets
        # even though kops doesn't support it yet.
        route_table = None
        for tmp_route_table in vpc.route_tables.all():
            if self._verify_cluster_tag(tmp_route_table.tags):
                route_table = tmp_route_table
                break

        if not route_table:
            logging.error("Could not find route table tagged with this cluster name")
            raise SystemError('Cluster route table not found')

        route_fixed = False
        for route in route_table.routes:
            if route.gateway_id == self.cluster['route_fix']['igw']:
                logging.info('Fixing route by adding ngw')
                route.replace(NatGatewayId=self.cluster['route_fix']['ngw'])
                route_fixed = True
                break

        if not route_fixed:
            logging.error('Did not fix route table')
            raise SystemError('Unable to fix route table')

    def _get_kops_config(self):
        return pykube.KubeConfig.from_file(self.kube_config)

    def _fixup_kube_config(self):
        """Add internal part of hostname."""

        logging.info('Fixing kubectl config')
        kops_config = self._get_kops_config()
        url = urlparse.urlparse(kops_config.cluster['server'])

        kops_config.cluster['server'] = "%s://%s" % (url.scheme, 'api.internal.' + url.netloc[4:])
        logging.info('Fixed cluster hostname %s', kops_config.cluster['server'])

        args = ['kubectl', ]
        args += ['config', 'set-cluster', self.cluster_name, '--kubeconfig=%s' % self.kube_config]
        args += ['--server=%s' % kops_config.cluster['server']]
        self._run_process(args)

    def _fix_security_group(self, instance_group, security_groups):
        """Apply a security group to a specific instance group and running instances."""

        logging.info('Fix instance group %s security group %s', instance_group, security_groups)

        asg_client = boto3.client('autoscaling')
        asg_info = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=['%s.%s' %
                                                                                  (instance_group, self.cluster_name)])
        if len(asg_info['AutoScalingGroups']) != 1:
            raise SystemError('Cannot find ASG')

        # Check kubernetes cluster tag to be safe
        if not self._verify_cluster_tag(asg_info['AutoScalingGroups'][0]['Tags']):
            raise SystemError('ASG not labled with cluster tag')

        lc_info = asg_client.describe_launch_configurations(LaunchConfigurationNames=
                                                            [asg_info['AutoScalingGroups']
                                                             [0]['LaunchConfigurationName']])

        if len(lc_info['LaunchConfigurations']) != 1:
            raise SystemError('Cannot find LaunchConfiguration')

        lc_name = '%s.%s-%s' % (instance_group, self.cluster_name, time.time())
        lc_info = lc_info['LaunchConfigurations'][0]

        asg_client.create_launch_configuration(LaunchConfigurationName=lc_name,
                                               ImageId=lc_info['ImageId'],
                                               KeyName=lc_info['KeyName'],
                                               SecurityGroups=lc_info['SecurityGroups'] + security_groups,
                                               UserData=base64.b64decode(lc_info['UserData']),
                                               InstanceType=lc_info['InstanceType'],
                                               BlockDeviceMappings=lc_info['BlockDeviceMappings'],
                                               InstanceMonitoring={
                                                   'Enabled': lc_info['InstanceMonitoring']['Enabled']
                                               },
                                               IamInstanceProfile=lc_info['IamInstanceProfile'],
                                               EbsOptimized=lc_info['EbsOptimized'],
                                               AssociatePublicIpAddress=lc_info['AssociatePublicIpAddress'])

        # Switch ASG to new LC
        asg_client.update_auto_scaling_group(AutoScalingGroupName='%s.%s' % (instance_group, self.cluster_name),
                                             LaunchConfigurationName=lc_name)
        # Delete old LC
        asg_client.delete_launch_configuration(LaunchConfigurationName=lc_info['LaunchConfigurationName'])

        # Now we can go fix the running EC2 instances
        ec2 = boto3.resource('ec2')

        ig_instances = ec2.instances.filter(Filters=[{'Name': 'tag:KubernetesCluster', 'Values' :[self.cluster_name]},
                                                     {'Name': 'instance-state-name', 'Values' :['pending', 'running']},
                                                     {'Name': 'tag:aws:autoscaling:groupName',
                                                      'Values': ['%s.%s' % (instance_group, self.cluster_name)]}])

        if len(list(ig_instances.all())) == 0:
            raise SystemError('No running instances found for this ASG')

        security_group_list = []
        for security_group in security_groups:
            security_group_list += {'GroupId': security_group}

        for instance in ig_instances:
            logging.info('Adding security groups to %s', instance.id)

            # Check cluster label to be safe
            if not self._verify_cluster_tag(instance.tags):
                raise SystemError('Instance not labled with cluster tag')

            # Build list of just sg ids of existing groups
            current_security_groups = [sg['GroupId'] for sg in instance.security_groups]
            instance.modify_attribute(Groups=current_security_groups + security_groups)

    def _fixup_security_groups(self):
        """Fix the security groups for running instances and launch configurations."""

        logging.info('Fixup security groups')
        for instance_group in self.cluster['instance_groups']:
            if 'security_groups' in self.cluster['instance_groups'][instance_group]:
                self._fix_security_group(instance_group,
                                         self.cluster['instance_groups'][instance_group]['security_groups'])

    def _label_public_subnets(self, should_label):
        """Sets/unsets KubernetesCluster label for public subnets."""

        # Public subents are required if k8s will be creating Internet/public facing
        # ELBs.  K8s will only attach ELBs to subnets with the right cluster label so we must apply the
        # label manually.

        logging.info('Set KubernetesCluster subnet label:%s', should_label)

        if 'unmanaged_public_subnets' in self.cluster['subnets']:
            for subnet_id in self.cluster['subnets']['unmanaged_public_subnets']:
                self._set_subnet_label(subnet_id, should_label)

    def _kops_create_cluster_args(self):
        """Create Popen process kops create args."""

        kops_args = [self.kops_bin, 'create', 'cluster']
        kops_args += ['--kubernetes-version', self.cluster['version']]
        kops_args += ['--master-size', self.cluster['master']['type']]
        kops_args += ['--master-zones', ','.join(self.cluster['master']['zones'])]
        kops_args += ['--dns-zone', self.cluster_config['dns_zone_id']]
        kops_args += ['--cloud', 'aws']
        kops_args += ['--node-count', self.cluster['instance_groups']['nodes']['number']]
        kops_args += ['--node-size', self.cluster['instance_groups']['nodes']['type']]
        kops_args += ['--zones', ','.join(self.cluster['subnets']['zones'])]
        kops_args += ['--state', 's3://' + self.cluster_config['s3_bucket']]
        kops_args += ['--network-cidr', '10.0.0.0/16']
        kops_args += ['--vpc', self.cluster_config['vpc_id']]
        kops_args += ['--admin-access', '10.0.0.0/16']
        kops_args += ['--image', self.cluster['ami_id']]
        kops_args += ['--ssh-public-key', self.cluster['ssh_key']]
        kops_args += ['--associate-public-ip=false']
        kops_args += [self.cluster_name]
        return kops_args

    def _kops_delete_cluster_args(self):
        """Create Popen process kops delete args."""

        kops_args = [self.kops_bin, 'delete', 'cluster', '--yes']
        kops_args += ['--state', 's3://' + self.cluster_config['s3_bucket']]
        kops_args += [self.cluster_name]
        return kops_args

    def _kops_update_cluster_args(self):
        """Create Popen process kops update args."""

        kops_args = [self.kops_bin, 'update', 'cluster', '--yes']
        kops_args += ['--state', 's3://' + self.cluster_config['s3_bucket']]
        kops_args += [self.cluster_name]
        return kops_args

    def _kops_version_args(self):
        """Create Popen process kops create args."""

        kops_args = [self.kops_bin, 'version']
        return kops_args

    def _get_total_nodes(self):
        """Calculate the total number of nodes in this cluster."""

        num = len(self.cluster['master']['zones'])

        for instance_group in self.cluster['instance_groups']:
            num += self.cluster['instance_groups'][instance_group]['number']
        return num

    @staticmethod
    def _run_process(args):
        """Runs a OS process and waits for it to exit"""

        args = [str(a) for a in args]

        logging.info("Running process:")
        logging.info(' ' .join(args))
        process = subprocess.Popen(args, close_fds=True)
        process.wait()
        if process.returncode != 0:
            logging.error('Non-zero return code %d', process.returncode)
            raise SystemError("Process returned non-zero")

    def _set_iam_policies(self, file_name, role, policy_name):
        """Updates all inline policies to rendered jinja policy file."""

        with open(file_name, 'r') as policy_file:
            policy_string = policy_file.read()

        deploy_template = jinja2.Template(policy_string)
        policy_string = deploy_template.render(cluster_name=self.cluster_name,
                                               aws_account=self.cluster_config['aws_account'],
                                               s3_bucket=self.cluster_config['s3_bucket'],
                                               dns_zone=self.cluster_config['dns_zone_id'])

        # Loop through inline policies for this role and update the one with the right name
        for policy in role.policies.all():
            if policy.policy_name == policy_name:
                logging.info("Updating policy %s", policy_name)
                policy.put(PolicyDocument=policy_string)
                return

        raise SystemError('Policy not found')

    def _set_subnet_label(self, subnet_id, should_label):
        """Sets/unsets KubernetesCluster label for a specific subnet id."""

        logging.info('Setting subnet label for %s', subnet_id)

        ec2 = boto3.resource('ec2')
        subnet = ec2.Subnet(subnet_id)
        subnet.load()

        if not self._verify_tag(subnet.tags, 'close.io/unmanaged_k8s_cluster', self.cluster_name):
            logging.error('Public subnet is not tagged correctly')
            raise SystemError('Public subnet not taggd correctly with close.io/unmanaged_k8s_cluster')

        if should_label:
            tag_value = self.cluster_name
        else:
            tag_value = ''

        subnet.create_tags(Tags=[{'Key': 'KubernetesCluster', 'Value': tag_value}])

    def _set_zones_for_instance_group(self, instance_group_name):
        """Set zones in state file for a instance group."""

        logging.info('Setting zones for instance group %s', instance_group_name)

        s3_resource = boto3.resource('s3')
        ig_s3_object = s3_resource.Object(self.config['environments'][self.environment]['config']['s3_bucket'],
                                          self.cluster_name + '/instancegroup/%s' % instance_group_name)

        ig_config = yaml.load(ig_s3_object.get()['Body'].read())

        ig_config['spec']['zones'] = self.cluster['instance_groups'][instance_group_name]['zones']

        # Update file in S3
        ig_s3_object.put(Body=yaml.safe_dump(ig_config, default_flow_style=False))

    def _validate_kube_config(self):
        """Confirms current kube config server url matches cluster name."""

        logging.info('Checking kube config')
        config = pykube.KubeConfig.from_file(self.kube_config)
        url = urlparse.urlsplit(config.cluster['server'])
        if url.hostname.lower() != 'api.internal.' + self.cluster_name.lower():
            logging.info('Kube config does not match cluster: %s %s', url.hostname.lower()[4:],
                         self.cluster_name.lower())
            return False

        return True

    @staticmethod
    def _verify_tag(tags, key, value):
        """Check if tag dict has tag and value matching this cluster."""

        for tag in tags:
            if tag['Key'] == key and tag['Value'] == value:
                return True
        return False

    def _verify_cluster_tag(self, tags):
        """Check if tag dict has tag and value matching this cluster."""

        return self._verify_tag(tags, 'KubernetesCluster', self.cluster_name)

    def _wait_for_cluster(self):
        """Wait until we can confirm all nodes have registered with cluster."""

        api = pykube.HTTPClient(self._get_kops_config())

        # We poll the cluster until the number of nodes it returns matches what we expect

        total_nodes = self._get_total_nodes()

        logging.info('Waiting for %d nodes', total_nodes)
        logging.info('This can take several minutes to complete')
        while True:
            try:
                nodes = pykube.Node.objects(api)
                if len(nodes.all()) == 0:
                    logging.info('No nodes available yet')
                    time.sleep(10)
                elif len(nodes.all()) > 0 and len(nodes.all()) < total_nodes:
                    logging.info('Some nodes available: %d of %d', len(nodes.all()), total_nodes)
                    time.sleep(2)
                elif len(nodes.all()) == total_nodes:
                    logging.info('All nodes available: %d', len(nodes.all()))
                    return
            except Exception, err:  #pylint: disable=W0703
                logging.info('Failed getting nodes')
                logging.info(err)
                time.sleep(10)

            time.sleep(2)



    def check_for_active_services(self):
        """Checks current cluster for active services"""

        logging.info('Checking for active services')

        api = pykube.HTTPClient(self._get_kops_config())

        namespaces = pykube.Namespace.objects(api).all()

        active_services = False
        for namespace in namespaces:
            logging.info('Checking namespace %s for service', namespace.name)
            if namespace.name == 'kube-system':
                logging.info('Skipping kube-system')
                continue

            services = pykube.Service.objects(api).filter(namespace='%s' % namespace.name)

            if len(services) > 0:
                logging.info('Active services: %d', len(services))
                for service in services:
                    if namespace.name == 'default' and service.name == 'kubernetes':
                        continue    #Skip this built in service
                    else:
                        logging.info('Service: %s', service.name)
                        active_services = True

        return active_services

    def check_for_running_pods(self):
        """Checks current cluster for running pods"""

        logging.info('Checking for running pods')

        api = pykube.HTTPClient(self._get_kops_config())

        namespaces = pykube.Namespace.objects(api).all()

        running_pods = False
        for namespace in namespaces:
            logging.info('Checking namespace %s for pods', namespace.name)
            if namespace.name == 'kube-system':
                logging.info('Skipping kube-system')
                continue

            pods = pykube.Pod.objects(api).filter(namespace='%s' % namespace.name)

            if len(pods) > 0:
                logging.info('Pods found running: %d', len(pods))
                for pod in pods:
                    logging.info('Pod: %s', pod.name)
                running_pods = True

        return running_pods

    def create_cluster_command(self):
        """Create k8s cluster"""

        start_deployment = time.time()

        # Check to see if a state folder for this cluster name already exists, fail if it does
        if self._check_for_cluster_in_s3():
            raise SystemError('S3 state folder already exists')

        # Check if any running instances are tagged with this cluster name, if so fail
        if self._check_for_ec2_instances():
            raise SystemError('EC2 Instances for this cluster already exist')

        # Build the command line parameters for the kops create cluster command
        kops_args = self._kops_create_cluster_args()

        # Run kops create cluster, this only creates the S3 state files
        self._run_process(kops_args)

        # Apply our configuration changes to the S3 state files
        self._fixup_cluster_config()

        # Create additional instance groups
        self._create_instance_groups()

        # Build the command line paramerts for the kops update cluster command
        kops_args = self._kops_update_cluster_args()

        # Run kops update cluster which actually creates the AWS resources
        self._run_process(kops_args)

        # Fix kube config to incluse internal hostname part
        self._fixup_kube_config()

        # Apply changes to infrastructure created by kops
        self._fixup_infrastructure()

        # Wait for cluster to come up
        # It will take several minutes for all of the nodes to spin up and bootstrap themselves
        self._wait_for_cluster()

        # Deploy addons
        self._deploy_addons()

        # Fixup IAM policies
        self._fixup_iam_policies()

        # Fixup Security groups
        self._fixup_security_groups()

        # Label public subnets
        self._label_public_subnets(True)

        end_deployment = time.time()
        logging.info('Cluster creation took %.2f minutes', (end_deployment-start_deployment)/60.0)

    def delete_cluster_command(self):
        """Delete k8s cluster."""

        if not self._check_for_cluster_in_s3():
            raise SystemError('S3 state folder does not exist')

        if not self._check_for_ec2_instances():
            logging.info('No running EC2 Instances for this cluster exist')


        if self.force_delete:
            logging.info('Valid kube config issue, force deleting')
        else:
            if not self._validate_kube_config():
                raise SystemError('Invalid kube config, must point to cluster to be deleted')

        if self.force_delete:
            logging.info('Force deleting, not checking cluster deployments and services')
        else:
            if self.check_for_running_pods():
                raise SystemError('Cluster has running pods that must be deleted before continuing')

            if self.check_for_active_services():
                raise SystemError('Cluster has active services that must be deleted before continuing')

        # Clear label on public subnets
        self._label_public_subnets(False)

        kops_args = self._kops_delete_cluster_args()
        self._run_process(kops_args)

    def diff_command(self):
        """Compare two cluster configs."""

        import difflib

        config2 = self.config['environments'][self.args.env_two]['clusters'][self.args.cluster_two]

        diff = difflib.unified_diff(yaml.safe_dump(self.cluster).split('\n'), yaml.safe_dump(config2).split('\n'),
                                    "%s-%s" % (self.args.environment, self.cluster_name),
                                    "%s-%s" % (self.args.env_two, self.args.cluster_two))
        for line in diff:
            print line

    def load_config(self):
        """Load cluster definition configuration file."""

        logging.info('Loading config file %s', self.config_filename)
        config_file = file(self.config_filename, 'r')
        config = yaml.load(config_file)
        self.config = config
        self.cluster = self.config['environments'][self.environment]['clusters'][self.cluster_name]
        self.cluster_config = self.config['environments'][self.environment]['config']

        # Confirm all cluster names are unique.  Since we tag AWS resources we want to be 100% sure
        # we don't accidently affect another cluster.
        logging.info('Checking for unique cluster names')
        cluster_names = []
        for cluster_environment in self.config['environments']:
            for cluster in self.config['environments'][cluster_environment]['clusters']:
                if cluster in cluster_names:
                    logging.error('Cluster name %s not unique!', cluster)
                    raise SystemError('Config must have unique cluster names')
                else:
                    cluster_names.append(cluster)

    def parse_args(self):
        """Parse command line arguments"""

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest='command')

        common_parser = argparse.ArgumentParser(add_help=False)

        # Global parameters
        common_parser.add_argument('-e', '--environment', help='Name of environment', required=True)
        common_parser.add_argument('-c', '--cluster-name', help='Name of cluster', required=True)
        common_parser.add_argument('-f', '--config-file', help='Config file', required=True)

        # Create command
        subparsers.add_parser('create', parents=[common_parser])

        # Delete command
        delete_parser = subparsers.add_parser('delete', parents=[common_parser])
        delete_parser.add_argument('-D', '--force', help='Force delete by skipping existing cluster check',
                                   required=False, action='store_true')

        # Status command
        subparsers.add_parser('status', parents=[common_parser])

        # Diff command
        diff_parser = subparsers.add_parser('diff', parents=[common_parser])
        diff_parser.add_argument('-e2', '--env-two', required=True)
        diff_parser.add_argument('-c2', '--cluster-two', required=True)

        # Parse arguments
        self.args = parser.parse_args()

        self.command = self.args.command
        self.environment = self.args.environment.strip()
        self.cluster_name = self.args.cluster_name.strip()
        # Paranoid check to make sure we have a reasonable name to match against cluster tags
        if len(self.cluster_name) < 6:
            logging.error('Cluster name is too short')
            raise SystemError('Cluster name is too short')
        self.config_filename = self.args.config_file
        if 'force' in self.args:
            self.force_delete = self.args.force

    def pre_check(self):
        """Test configuration before starting operations."""

        logging.info("Starting health check")

        try:
            # Print kops version for debugging
            kops_args = self._kops_version_args()
            self._run_process(kops_args)
        except:  #pylint: disable=W0702
            logging.info("Could not run kops")
            logging.debug(sys.exc_info())

        user_session = None
        sts_session = None
        aws_account = self.config['environments'][self.environment]['config']['aws_account']
        try:
            user = boto3.client('iam').get_user()
            if user['User']['Arn'].split(':')[4] == aws_account:
                user_session = True
                logging.info("Confirmed user AWS account")
            else:
                user_session = False
                logging.error("User credentials not configured for correct AWS account. Requested:%s Found:%s",
                              user['User']['Arn'].split(':')[4], aws_account)
        except: #pylint: disable=W0702
            logging.info("Does not look like we are logged in as normal user")
            logging.debug(sys.exc_info())

        try:
            caller = boto3.client('sts').get_caller_identity()
            if caller['Account'] == aws_account:
                sts_session = True
                logging.info("Confirmed STS AWS account")
            else:
                sts_session = False
                logging.error("STS credentials not configured for correct AWS account. Requested:%s Found:%s",
                              caller['Account'], aws_account)
        except: #pylint: disable=W0702
            logging.info("Does not look like we have a STS session")
            logging.debug(sys.exc_info())

        if not user_session and not sts_session:
            logging.error("No valid AWS credentials found")
            raise SystemError('No AWS credentials')

    def run_command(self):
        """Run command."""

        # Run appropriate command
        if self.command == 'create':
            self.create_cluster_command()
        elif self.command == 'delete':
            self.delete_cluster_command()
        elif self.command == 'status':
            self.status_command()
        elif self.command == 'diff':
            self.diff_command()

    def status_command(self):
        """Print status of cluster."""

        logging.info('Checking status')
        try:
            logging.info('S3 state folder: %s', self._check_for_cluster_in_s3())
        except Exception:  #pylint: disable=W0703
            logging.info('S3 state folder failed')

        try:
            logging.info('Running EC2 cluster instances: %s', self._check_for_ec2_instances())
        except Exception:  #pylint: disable=W0703
            logging.info('Running EC2 cluster failed')

        try:
            logging.info('Valid kube config: %s', self._validate_kube_config())
        except Exception:  #pylint: disable=W0703
            logging.info('Valid kube config failed')

        try:
            logging.info('Running pods: %s', self.check_for_running_pods())
        except Exception:  #pylint: disable=W0703
            logging.info('Running pods failed')



def run():
    """Run cluster management"""

    init()
    logging.info("Starting cluster management")
    manage_cluster = ManageCluster()

    # Parse args passed to program
    manage_cluster.parse_args()

    # Load the cluster configuration file
    manage_cluster.load_config()

    # Perform some health checks before we get started
    manage_cluster.pre_check()

    # Run the command passed on command line
    manage_cluster.run_command()

    logging.info("Done cluster management")

if __name__ == "__main__":
    try:
        run()
    except Exception as exception:  #pylint: disable=w0703
        logging.error("Error:", exc_info=True)
        sys.exit(1)
