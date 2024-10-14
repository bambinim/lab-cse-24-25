#!/bin/bash

function executeCommand {
    "$@"
    if [[ $? != '0' ]] ; then
        echo "ERROR! Command failed" > /dev/stderr
        exit 1
    fi
}

echo "Configuring AWS profile"
executeCommand aws configure --profile "${AWS_PROFILE}"