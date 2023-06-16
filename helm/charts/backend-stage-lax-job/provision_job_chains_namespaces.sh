#!/bin/bash
#
# Usage: ./provision_job_chains_namespaces.sh.sh [env(prod|stage)] [deployment] [tag]
#KUBE_NAMESPACE="$1-indexation-polygon-137 $1-indexation-bsc-56 $1-indexation-fantom-250 $1-indexation-arbitrum-42161 $1-indexation-optimism-10 $1-indexation-gnosis-100 $1-indexation-canto-7700 $1-indexation-nova-42170 $1-indexation-base-84531"
KUBE_NAMESPACE="$1-indexation-canto-7700"

JOB_NAME=$3
CI_COMMIT_SHORT_SHA=$2

for NAMESPACE in ${KUBE_NAMESPACE}; do
  CHAIN=$(echo $NAMESPACE | sed -En "s/.*-indexation-//p")
  echo "...."
  echo "Job ${JOB_NAME} deploy to ${NAMESPACE} namespace"
  echo " "
  echo " "
      if [[ "$CHAIN" ]] ; then
        helm upgrade --install ${JOB_NAME} ./ --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${JOB_NAME} --set chainName=${CHAIN} --set kubeNamespace=${NAMESPACE} --history-max=2
            else
        helm upgrade --install ${JOB_NAME} ./ --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${JOB_NAME} --set kubeNamespace=${NAMESPACE} --history-max=2
      fi
  echo "Deployed ${JOB_NAME} in ${NAMESPACE}"
done
