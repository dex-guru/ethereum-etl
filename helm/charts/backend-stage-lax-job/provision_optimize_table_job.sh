#!/usr/bin/env bash
NAMESPACE=stage-indexation-canto-7700
TAG=f97397c4
JOB_NAMES=("optimize-tables")
for JOB_NAME in ${JOB_NAMES[@]}; do
  helm upgrade --install ${JOB_NAME} ./ --wait -n ${NAMESPACE} --set imageTag=${TAG} --set appName=${JOB_NAME} --set kubeNamespace=${NAMESPACE}
done
