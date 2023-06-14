
1. You need to install kubectl:
```bash 
brew install kubectl
```

2. Get the kubeconfig file from the cluster and put it from https://control.dexguru.biz/dashboard

4. export KUBECONFIG variable with the path to the kubeconfig file
```bash
export KUBECONFIG=/Users/username/Downloads/kubeconfig.yaml
```

Provisioning optimize jobs:

``` bash
./provision_job_chains_namespaces.sh stage afd0c372 optimize-tables    

```

Removing Optimize Jobs:

```bash
./uninstall_from_chains_namespaces.sh stage optimize-tables   
```