oc delete template oshinko-webui
oc delete serviceaccount oshinko
oc delete rolebindings oshinko
oc delete all --all
 oc new-app -f ../spark-operator-template.yaml