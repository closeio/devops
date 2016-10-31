"""CICD Processing."""

import argparse
import base64
import datetime
import logging
import os
import os.path
import subprocess
import sys
import time

import boto3
import jinja2
import yaml

from deploy import k8s_deploy_from_file, k8s_deploy_from_manifest
from ecr_cleaner import prune_ecr


def init():
    """Initialize system."""

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


class CICDProcessor(object):
    """CICD Processor class."""

    def __init__(self):

        self.directory = None
        self.filename = None
        self.phases = None
        self.variables = None
        self.process_subdirs = False

    @staticmethod
    def _run_process(args, ignore_error=False, timeout=240, shell=False):
        """Runs a OS process and waits for it to exit"""

        args = [str(a) for a in args]

        logging.info("Running process:")
        logging.info(' ' .join(args))
        if shell:
            logging.info('Via shell')
            args = ' '.join(args)
        process = subprocess.Popen(args, close_fds=True, shell=shell)
        start_time = datetime.datetime.now()
        while process.returncode is None:
            time.sleep(.5)
            process.poll()
            if (datetime.datetime.now() - start_time).total_seconds() > timeout:
                raise ProcessingError('Timeout running command')
        if not ignore_error and process.returncode != 0:
            logging.error('Non-zero return code %d', process.returncode)
            raise ProcessingError('Process returned non-zero')

    def command_docker(self, service_directory, settings):
        """Run docker command."""

        logging.info('Running docker command')

        cwd = os.getcwd()
        os.chdir(service_directory)

        args = [
            'docker'
        ] + list(settings['args'])

        if 'ignore_error' in settings and settings['ignore_error']:
            ignore_error = True
        else:
            ignore_error = False

        self._run_process(args, ignore_error, shell=True, timeout=self.get_command_timeout(settings))
        os.chdir(cwd)

    def command_k8s_config_map(self, service_directory, settings):
        """Deploy to k8s."""

        manifest = """
        kind: ConfigMap
        apiVersion: v1
        metadata:
          name: %s
          namespace: %s
        data: {}
        """ % (settings['name'], settings['namespace'])

        manifest = yaml.load(manifest)

        for i in range(0, len(settings['data'])):
            value = list(settings['data'][i].keys())[0]
            if value == 'file':
                with open(settings['data'][i][value]['name']) as config_file:
                    manifest['data'][settings['data'][i][value]['key']] = config_file.read()
            elif value == 'value':
                manifest['data'][settings['data'][i][value]['key']] = settings['data'][i][value]['data']
            else:
                raise ProcessingError('Unknown config map data type')

        k8s_deploy_from_manifest(self.variables['KUBE_CONFIG'], manifest, self.variables['VERSION'],
                                 timeout=self.get_command_timeout(settings))

    def command_k8s_deploy(self, service_directory, settings):
        """Deploy to k8s."""

        k8s_deploy_from_file(self.variables['KUBE_CONFIG'], settings['manifest'], self.variables['VERSION'],
                             self.variables, timeout=self.get_command_timeout(settings))

    def command_run(self, service_directory, settings):
        """Run bash script."""

        logging.info('Running run command')

        cwd = os.getcwd()
        os.chdir(service_directory)

        name = settings['command']
        args = []
        if 'args' in settings:
            args = list(settings['args'])
        args = [name] + args

        self._run_process(args, timeout=self.get_command_timeout(settings))
        os.chdir(cwd)

    def command_script(self, service_directory, settings):
        """Run bash script."""

        logging.info('Running script command')

        cwd = os.getcwd()
        os.chdir(service_directory)

        name = settings['name']
        args = []
        if 'args' in settings:
            args = list(settings['args'])
        args = ['/bin/bash', service_directory + '/' + name] + args

        self._run_process(args, timeout=self.get_command_timeout(settings))
        os.chdir(cwd)

    def command_ecr_login(self, service_directory, repo):
        client = boto3.client('ecr')

        response = client.get_authorization_token()
        ecr_token = response['authorizationData'][0]['authorizationToken']

        ecr_token = base64.b64decode(ecr_token)

        # Python 3
        if type(ecr_token) == bytes:
            ecr_token = ecr_token.decode('utf-8')

        ecr_token = ecr_token.split(':')

        args = {'args': ['login',
                         '-u',
                         ecr_token[0],
                         '-p',
                         ecr_token[1],
                         '-e',
                         'none',
                         repo
                         ]}

        self.command_docker(service_directory, args)

    def command_prune_ecr(self, service_directory, settings):

        prune_ecr(settings['region'], str(settings['account']), settings['name'], settings['days'], settings['min_num'])

    def get_command_timeout(self, settings):
        """Get timeout in config sections."""

        if 'timeout' in settings:
            return settings['timeout']
        else:
            return 240

    def get_service_files(self):
        """Return ordered list of service files."""

        dir_list = os.listdir(self.directory)

        service_files = []

        # Look in each subdirectory for a services yaml file
        for f in dir_list:
            service_directory = self.directory + '/' + f
            if os.path.isdir(service_directory):
                deploy_file = service_directory + '/' + self.filename
                if os.path.isfile(deploy_file):
                    service_files.append((deploy_file, self.get_service_order(deploy_file)))

        # Sort service files based on order number
        service_files = sorted(service_files, key=lambda tup: tup[1])
        service_files = [tup[0] for tup in service_files]
        logging.info('Processing the following service files:')
        for service_file in service_files:
            logging.info('   %s', service_file)

        return service_files

    def get_service_order(self, service_file):
        """Get priority our of service file."""

        self.set_service_variable_defaults(service_file)
        with open(service_file, 'r') as config_file:
            config_yaml = self.render_config(config_file.read())

        if 'config' in config_yaml and 'order' in config_yaml['config']:
            order = config_yaml['config']['order']
        else:
            order = 100

        return order

    def parse_args(self):
        """Parse command line arguments"""

        parser = argparse.ArgumentParser()

        parser.add_argument('-p', '--phase', help='Phases to run in deploy file.  Comma separated.', required=True)
        parser.add_argument('-d', '--dir', help='Directory to scan for deploy files', required=True)
        parser.add_argument('-f', '--filename', help='Deployment file name (default: service.yaml)',
                            default='service.yaml', required=False)
        parser.add_argument('-s', '--subdirs', help='Process subdirectories', required=False, action='store_true')

        parser.add_argument('-v', '--variable', required=False, action='append',
                            help='Format var1=value1. Multiple variables are allowed.')

        # Parse arguments
        args = parser.parse_args()

        self.directory = os.path.realpath(args.dir)
        self.filename = args.filename
        self.phases = args.phase.split(',')
        self.process_subdirs = args.subdirs

        # Build variables dictionary based on variables passed on command line
        self.variables = {}
        if args.variable:
            for variable in args.variable:
                values = variable.split('=')
                self.variables[values[0]] = values[1]

    def process_services(self, phase):
        """Process directory for deploy files."""

        logging.info('Processing directory: %s', self.directory)

        # If a service file exists in the diretory we just run that one
        if os.path.isfile(self.directory + '/' + self.filename):
            logging.info('Processing single directory')
            self.run_cicd_phase(phase, self.directory + '/' + self.filename)
        elif self.process_subdirs:
            # Look for subdirectories with service files
            logging.info('Processing subdirectories')

            service_files = self.get_service_files()

            for service_file in service_files:
                self.run_cicd_phase(phase, service_file)
        else:
            logging.info('No service found, not processing subdirectories')

    def render_config(self, config):
        """Render service config using jinja."""

        config_template = jinja2.Template(str(config))
        config_template.environment.undefined = jinja2.StrictUndefined
        config_string = config_template.render(self.variables)

        return yaml.load(config_string)

    def run(self):
        """Run processor."""

        self.parse_args()

        # Set default variables
        if 'VERSION' not in self.variables:
            self.variables['VERSION'] = '{:%Y%m%d%H%M%S}'.format(datetime.datetime.now())
            logging.info('Setting VERSION:%s', self.variables['VERSION'])

        if 'KUBE_CONFIG' not in self.variables:
            self.variables['KUBE_CONFIG'] = os.path.realpath(os.path.expanduser('~/.kube/config'))
            logging.info('Set kube config to:%s', self.variables['KUBE_CONFIG'])
        else:
            self.variables['KUBE_CONFIG'] = os.path.realpath(os.path.expanduser(self.variables['KUBE_CONFIG']))

        for phase in self.phases:
            logging.info('Running phase:%s', phase)
            self.process_services(phase)

        logging.info('Done version:%s', self.variables['VERSION'])

    def run_cicd_phase(self, phase, deploy_file):
        """Run a phase in a deployment file."""

        logging.info('Run phase:%s in file:%s', phase, deploy_file)

        service_directory = self.set_service_variable_defaults(deploy_file)

        cwd = os.getcwd()
        os.chdir(service_directory)
        with open(deploy_file, 'r') as config_file:
            config_yaml = self.render_config(config_file.read())

        if phase not in config_yaml['phases']:
            logging.debug('Phase not in cicd file')
            return

        if type(config_yaml['phases'][phase]) != list:
            logging.error('Must be list of commands so we can ensure order.')
            raise ProcessingError('Must be list')

        for i in range(0, len(config_yaml['phases'][phase])):
            self.run_command(service_directory, config_yaml['phases'][phase][i])

        os.chdir(cwd)

    def run_command(self, service_directory, command_section):
        """Run a specific command."""

        if type(command_section) == str:
            command = command_section
            section = None
        else:
            command = list(command_section.keys())[0]
            section = command_section[command]

        logging.info('Running command %s', command)

        switcher = {
            'docker': self.command_docker,
            'ecr_login': self.command_ecr_login,
            'k8s_config_map': self.command_k8s_config_map,
            'k8s_deploy': self.command_k8s_deploy,
            'prune_ecr': self.command_prune_ecr,
            'run': self.command_run,
            'script': self.command_script
        }

        the_command = switcher.get(command, None)

        if the_command:
            the_command(service_directory, section)
        else:
            logging.error('Unknown command:%s', command)
            raise ProcessingError('Unknown command %s', command)

    def set_service_variable_defaults(self, deploy_file):
        """Set service variable defaults."""

        service_directory = os.path.dirname(os.path.realpath(os.path.expanduser(deploy_file)))
        self.variables['SERVICE_DIRECTORY'] = service_directory

        return service_directory


class ProcessingError(Exception):
    pass


def run():
    """Run CICD phase."""

    init()
    logging.info('Starting CICD processor')
    processor = CICDProcessor()
    processor.run()

    logging.info('Finished CICD processor')


if __name__ == "__main__":
    try:
        run()
    except Exception as exception:  # pylint: disable=w0703
        logging.error("Error:", exc_info=True)
        sys.exit(1)
