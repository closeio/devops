#!/bin/bash

# This script will run a deployment looking for the service config file in the current directory
# Its typically used to deploy services to a k8s cluster from a local workstation

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${SCRIPT_DIR}/k8scicd.sh -p build,push,deploy,clean -d . $@
