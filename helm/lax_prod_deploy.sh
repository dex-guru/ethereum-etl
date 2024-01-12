#!/bin/bash
#set -eux

kubectl config set-cluster ${LAX_PROD_CLUSTER_NAME} --server=${DG_PROD_LAX_K8S_API} --insecure-skip-tls-verify=true
kubectl config set-credentials ${LAX_PROD_CLUSTER_NAME} --token=${DG_PROD_LAX_CI_TOKEN}
kubectl config set-context ${LAX_PROD_CLUSTER_NAME} --cluster=${LAX_PROD_CLUSTER_NAME} --user=${LAX_PROD_CLUSTER_NAME}
kubectl config use-context ${LAX_PROD_CLUSTER_NAME}

#helm lint helm/charts/backend-stage-hz --set appName=${SERVICE_NAME} --set imageTag=${CI_COMMIT_SHORT_SHA} || fail=1

for NAMESPACE in ${KUBE_NAMESPACE}; do
    CHAIN=$(echo $NAMESPACE | sed -En "s/.*-indexation-//p")
    echo ">>>Start ${NAMESPACE}"
    # place "NotFound" message into IsRegistrySecretCreated if kubectl cannot get registry-gitlab secret
    IsRegistrySecretCreated=$(kubectl get secret registry-gitlab --namespace=$NAMESPACE 2>&1 | grep NotFound)
    echo "Service ${SERVICE_NAME} deploy to ${NAMESPACE} namespace"
    echo " "
    if [[ "$CHAIN" ]] ; then
      helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait --create-namespace -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} --history-max=2
    else
      helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait --create-namespace -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --history-max=2
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
        helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait --create-namespace -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} --history-max=2|| fail=1
      else
        helm upgrade --install ${SERVICE_NAME} ${HELM_CHART} --wait --create-namespace -n ${NAMESPACE} --set imageTag=${CI_COMMIT_SHORT_SHA} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --history-max=2|| fail=1
      fi
      echo " "
        helm history ${SERVICE_NAME} -n ${NAMESPACE}
      echo "--- END"
    fi
    if [[ "$IsRegistrySecretCreated" != "" ]] ; then
      echo "registry-gitlab secret not found, adding registry-gitlab secret"
      kubectl create secret docker-registry --namespace=${NAMESPACE} registry-gitlab --docker-server=${REGISTRY_HOST} --docker-username=${REGISTRY_USER} --docker-password=${REGISTRY_PASSWORD}
    else
      echo "registry-gitlab secret found, nothing to do here"
      echo "${IsRegistrySecretCreated}"
    fi
    echo ">>>Done ${NAMESPACE}"
done

  if [ "$fail" ] ; then
    exit 1
  fi