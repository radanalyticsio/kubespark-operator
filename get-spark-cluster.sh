oc get sparkclusters -o custom-columns=NAME:metadata.name,WORKER:spec.workers,MASTER_URL:"spec.SparkMasterName",IMAGE:"spec.Image"
