#!/bin/bash
NAMESPACE=prod
JOB_NAMES=("clone-candle-indexes-25")
for JOB_NAME in ${JOB_NAMES[@]}; do
  echo ${JOB_NAME}
  helm uninstall ${JOB_NAME} -n=${NAMESPACE}
done

JOB_NAMES=("clone-transactions")
for JOB_NAME in ${JOB_NAMES[@]}; do
  helm uninstall ${JOB_NAME} -n=${NAMESPACE}
done
