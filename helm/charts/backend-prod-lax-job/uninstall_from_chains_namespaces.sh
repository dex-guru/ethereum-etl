#!/bin/bash
#
# Rollback helm deployment in all networks namespaces
# Usage: ./uninstall_from_chains_namespaces.sh [env(prod|stage)] [deployment]
# Example:./uninstall_from_chains_namespaces.sh prod clone-es-candles-last-two-weeks-for-chain
KUBE_NAMESPACE="$1-indexation-eth-1 $1-indexation-bsc-56 $1-indexation-avalanche-43114 $1-indexation-polygon-137 $1-indexation-fantom-250 $1-indexation-arbitrum-42161 $1-indexation-nova-42170 $1-indexation-celo-42220 $1-indexation-optimism-10 $1-indexation-gnosis-100 $1-indexation-canto-7700 $1-indexation-acanto-7701 $1-indexation-base-84531"
#KUBE_NAMESPACE="$1-indexation-base-84531 $1-indexation-optimism-10"

for NAMESPACE in ${KUBE_NAMESPACE}; do
  echo "Uninstalling $2 deployment in ${NAMESPACE}"
  helm uninstall $2 -n ${NAMESPACE}
done