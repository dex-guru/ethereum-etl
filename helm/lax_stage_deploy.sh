#!/bin/bash
#set -eux

kubectl config set-cluster ${LAX_STAGE_CLUSTER_NAME} --server=${DG_STAGE_LAX_K8S_API} --insecure-skip-tls-verify=true
kubectl config set-credentials ${LAX_STAGE_CLUSTER_NAME} --token=${DG_STAGE_LAX_CI_TOKEN}
kubectl config set-context ${LAX_STAGE_CLUSTER_NAME} --cluster=${LAX_STAGE_CLUSTER_NAME} --user=${LAX_STAGE_CLUSTER_NAME}
kubectl config use-context ${LAX_STAGE_CLUSTER_NAME}

#helm lint helm/charts/backend-stage-hz --set appName=${SERVICE_NAME} --set imageTag=${CI_COMMIT_SHORT_SHA} || fail=1 

for NAMESPACE in ${KUBE_NAMESPACE}; do
    CHAIN=$(echo $NAMESPACE | sed -En "s/.*-indexation-//p")
    echo "...."
    echo "Service ${SERVICE_NAME} deploy to ${NAMESPACE} namespace"
    echo "!!!!!!!!!"
    helm show all ${HELM_CHART}
    echo "!!!!!!!!!"
      if [[ "$CHAIN" ]] ; then
        helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} --history-max=2
      else
        helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --history-max=2
      fi
    echo " "
        helm history ${SERVICE_NAME} -n ${NAMESPACE}
        revision_status=$(helm history ${SERVICE_NAME} -n ${NAMESPACE} --max=1 | tail -n 1 | awk '{print $7}')
    echo " "
          if [[ "$revision_status" != "deployed" ]] ; then
            echo "--- WARNING"
            echo "--- Past deploy status is - ${revision_status}"
            echo "--- Delete ${SERVICE_NAME} service and redeploy it to ${NAMESPACE} namespace"
              helm uninstall ${SERVICE_NAME} -n ${NAMESPACE}
            echo " "
            if [[ "$CHAIN" ]] ; then 
              helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} --history-max=2|| fail=1 
            else
              helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --history-max=2|| fail=1
            fi
            echo " "
              helm history ${SERVICE_NAME} -n ${NAMESPACE}
            echo "--- END"
          fi

    echo "...."
done 

echo "Kibana: https://kibana-stage-nur1.dexguru.biz/app/discover"
echo "Monitoring: https://grafana-stage-nur1.dexguru.biz/d/pod_metrics_aggregated/k8s-pod-metrics-aggregated?orgId=1&refresh=1m"

  if [ "$fail" ] ; then
    exit 1
  fi
