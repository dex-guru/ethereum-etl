#!/bin/bash
set -eu

fail=0

kubectl config set-cluster ${CLUSTER_NAME} --server=${K8S_API} --insecure-skip-tls-verify=true
kubectl config set-credentials ${CLUSTER_NAME} --token=${CI_TOKEN}
kubectl config set-context ${CLUSTER_NAME} --cluster=${CLUSTER_NAME} --user=${CLUSTER_NAME}
kubectl config use-context ${CLUSTER_NAME}

SECRET_NAME="registry-harbor"
REGISTRY_HOST=${CI_HARBOR_REGISTRY_HOST}
REGISTRY_USERNAME=${CI_HARBOR_REGISTRY_USER}
REGISTRY_PASSWORD=${CI_HARBOR_REGISTRY_PASSWORD}
ENCODED_AUTH=$(echo -n "${REGISTRY_USERNAME}:${REGISTRY_PASSWORD}" | base64)

# Generate the desired .dockerconfigjson content
NEW_DOCKERCONFIGJSON=$(echo -n "{\"auths\":{\"${REGISTRY_HOST}\":{\"username\":\"${REGISTRY_USERNAME}\",\"password\":\"${REGISTRY_PASSWORD}\",\"auth\":\"${ENCODED_AUTH}\"}}}" | base64 -w 0)

for NAMESPACE in ${KUBE_NAMESPACE}; do
  # Check for the existence of the namespace
    if ! kubectl get namespace ${NAMESPACE} &> /dev/null; then
        echo "Namespace ${NAMESPACE} not found, creating it..."
        kubectl create namespace ${NAMESPACE}
        echo "Namespace ${NAMESPACE} created."
    else
        echo "Namespace ${NAMESPACE} already exists, skipping creation."
    fi

    # Fetch the existing secret
    CURRENT_DOCKERCONFIGJSON=$(kubectl get secret ${SECRET_NAME} -n ${NAMESPACE} -o jsonpath='{.data.\.dockerconfigjson}' 2>/dev/null || echo "")

    # Determine if secret exist/needs updating
    if [ "${NEW_DOCKERCONFIGJSON}" != "${CURRENT_DOCKERCONFIGJSON}" ]; then
        kubectl delete secret ${SECRET_NAME} -n ${NAMESPACE} || true
        kubectl create secret docker-registry registry-harbor \
            --docker-server="${CI_HARBOR_REGISTRY_HOST}" \
            --docker-username="${CI_HARBOR_REGISTRY_USER}" \
            --docker-password="${CI_HARBOR_REGISTRY_PASSWORD}" \
            -n ${NAMESPACE}
        echo "Secret 'registry-harbor' re/created."
    fi
#    # Check for the existence of the 'registry-harbor' secret
#     if ! kubectl get secret registry-harbor -n ${NAMESPACE} &> /dev/null; then
#         echo "Secret 'registry-harbor' not found in ${NAMESPACE}, creating it..."
#         kubectl create secret docker-registry registry-harbor \
#             --docker-server="${CI_HARBOR_REGISTRY_HOST}" \
#             --docker-username="${CI_HARBOR_REGISTRY_USER}" \
#             --docker-password="${CI_HARBOR_REGISTRY_PASSWORD}" \
#             -n ${NAMESPACE}
#         echo "Secret 'registry-harbor' created."
#     else
#         echo "Secret 'registry-harbor' already exists in ${NAMESPACE}, skipping creation."
#     fi
    CHAIN=$(echo $NAMESPACE | sed -En "s/.*-indexation-//p")
    echo "...."
    echo "Service ${APP_NAME} deploy to ${NAMESPACE} namespace"
    if [[ "$CHAIN" ]] ; then
        echo "Running upgrade for chain namespace"
        if ! helm upgrade --install ${APP_NAME} ${HELM_CHART} --wait --timeout 10m -n ${NAMESPACE} --set imageTag=${IMAGE_TAG} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} -f helm/envs/${SERVICE_NAME}-${ENVIRONMENT}/${CHAIN}.yaml -f ${SERVICE_ENV_VALUE} --history-max=2; then
            echo "Helm upgrade failed for chain namespace"
            fail=1
        fi
    else
        echo "Running upgrade for ${NAMESPACE} namespace"
        if ! helm upgrade --install ${APP_NAME} ${HELM_CHART} --wait --timeout 10m -n ${NAMESPACE} --set imageTag=${IMAGE_TAG} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} -f helm/envs/${SERVICE_NAME}-${ENVIRONMENT}/${CHAIN}.yaml -f ${SERVICE_ENV_VALUE} --history-max=2; then
            echo "Helm upgrade failed for ${NAMESPACE} namespace"
            fail=1
        fi
    fi
    echo " "
    helm history ${APP_NAME} -n ${NAMESPACE}
    revision_status=$(helm history ${APP_NAME} -n ${NAMESPACE} --max=1 | tail -n 1 | awk '{print $7}')
    echo " "
    if [[ "$revision_status" != "deployed" ]] ; then
        echo "--- WARNING"
        echo "--- Past deploy status is - ${revision_status}"
        echo "--- Delete ${APP_NAME} service and redeploy it to ${NAMESPACE} namespace"
        helm uninstall ${APP_NAME} -n ${NAMESPACE}
        echo " "
        if [[ "$CHAIN" ]] ; then 
            if ! helm upgrade --install ${APP_NAME} ${HELM_CHART} --wait --timeout 10m -n ${NAMESPACE} --set imageTag=${IMAGE_TAG} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} --set chainName=${CHAIN} -f helm/envs/${SERVICE_NAME}-${ENVIRONMENT}/${CHAIN}.yaml -f ${SERVICE_ENV_VALUE} --history-max=2; then
                echo "Helm re-upgrade failed for chain namespace"
                fail=1
            fi
        else
            if ! helm upgrade --install ${APP_NAME} ${HELM_CHART} --wait --timeout 10m -n ${NAMESPACE} --set imageTag=${IMAGE_TAG} --set appName=${SERVICE_NAME} --set kubeNamespace=${NAMESPACE} -f helm/envs/${SERVICE_NAME}-${ENVIRONMENT}/${CHAIN}.yaml -f ${SERVICE_ENV_VALUE} --history-max=2; then
                echo "Helm re-upgrade failed for ${NAMESPACE} namespace"
                fail=1
            fi
        fi
        echo " "
        helm history ${APP_NAME} -n ${NAMESPACE}
        echo "--- END"
    fi

    echo "...."
done

# Check if any helm upgrade failed
if [ "$fail" -eq 1 ]; then
    echo "One or more helm upgrades failed. Exiting with error."
    exit 1
fi
