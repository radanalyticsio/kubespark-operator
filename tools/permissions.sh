export NS=myproject
export SRV_ACC=oshinko
oc create sa oshinko
oc create clusterrolebinding root-cluster-admin-binding --clusterrole=cluster-admin --serviceaccount=$NS:$SRV_ACC
