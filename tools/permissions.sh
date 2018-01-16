export NS=myproject
export SRV_ACC=oshinko
oc create clusterrolebinding default-admin --clusterrole=cluster-admin --serviceaccount=$NS:$SVC_ACC
