# kubespark-operator

Since kubernetes 1.7 CRD's were introduced to provide developers with a way to inherit all the features of kubernetes and build custom components on top. In this article I plan on introducing you to custom resource definitions and how you can utilize spark operator to manage your spark cluster on OpenShift.
What is a CRD?
Custom Resource Definition - This is a native kubernetes feature that lets you create kubernetes managed objects that you can manage. This is a replacement for third party resources. The main motivation for utilizing custom resource definitions is that will allow people to prototype new features without having to set up community meetings and proposals. 
Two ways to create CRD's:
#### With Yaml:
```yaml
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  creationTimestamp: 2018-01-16T18:18:37Z
  name: sparkclusters.radanalytics.redhat.com
  resourceVersion: "3746"
  selfLink: /apis/apiextensions.k8s.io/v1beta1/customresourcedefinitions/sparkclusters.radanalytics.redhat.com
  uid: ab9841a9-fae9-11e7-bae2-0862664fea2e
spec:
  group: radanalytics.redhat.com
  names:
  kind: SparkCluster
  listKind: SparkClusterList
  plural: sparkclusters
  singular: sparkcluster
  scope: Namespaced
  version: v1
status:
  acceptedNames:
  kind: SparkCluster
  listKind: SparkClusterList
  plural: sparkclusters
  singular: sparkcluster
  conditions:
  - lastTransitionTime: null
  message: no conflicts found
  reason: NoConflicts
  status: "True"
  type: NamesAccepted
  - lastTransitionTime: 2018-01-16T18:18:37Z
  message: the initial names have been accepted
  reason: InitialNamesAccepted
  status: "True"
  type: Established
  ```
Via golang code: 
/crd/SparkClusterCRD.go

 
Installation of Spark Operator
```bash
 oc create -f https://raw.githubusercontent.com/radanalyticsio/kubespark-operator/master/manifests/sparkcluster-operator.yaml
 oc new-app spark-operator
```

### What is an Operator/Controller?
The operator aka controller is event driven and watches sparkcluster creations/updates/deletions. 
  
* Create the following file as my-spark-cluster.yaml:

```yaml
apiVersion: radanalytics.redhat.com/v1
kind: SparkCluster
metadata:
  clusterName: "trevor"
  labels:
    radanalytics: sparkcluster
  name: "trevor"
  namespace: myproject
spec:
  Image: radanalyticsio/openshift-spark:2.2-latest
  SparkMasterName: trevor-spark-master
  SparkWorkerName: trevor-spark-worker
  sparkmetrics: prometheus
  workers: 5
status:
  message: Created, not processed yet
  state: created
  ```
Create:
You can create a spark cluster execute the following command:
```bash
 oc create -f manifests/spark-cluster.yaml 
```
Delete:
You can delete a spark cluster execute the following command:
```bash 
oc delete sparkcluster trevor
```
Scale:
You can scale up and down a spark cluster via the following command by changing the number of workers.:
```bash
oc edit sparkcluster trevor
``` 
 
Note: Spark operator will create the necessary pods and services required to run your analytics cluster. To see details run `oc get all` see output below
```bash
oc get all
```
output:
```bash
NAME READY STATUS RESTARTS AGE
po/prometheus-trevor-spark-master-1148891927-mrns7 1/1 Running 0 9s
po/trevor-spark-master-3438774284-w5blz 1/1 Running 0 9s
po/trevor-spark-worker-705698751-dvpxn 1/1 Running 0 9s
po/trevor-spark-worker-705698751-g8fp7 1/1 Running 0 9s
po/trevor-spark-worker-705698751-pghbq 1/1 Running 0 9s
po/trevor-spark-worker-705698751-s62jj 1/1 Running 0 9s
po/trevor-spark-worker-705698751-vwcc2 1/1 Running 0 9s
NAME CLUSTER-IP EXTERNAL-IP PORT(S) AGE
svc/prometheus-trevor-spark-master-service None <none> 9090/TCP 9s
svc/trevor-spark-master-service None <none> 7077/TCP,7777/TCP,8080/TCP 9s
NAME DESIRED CURRENT UP-TO-DATE AVAILABLE AGE
deploy/prometheus-trevor-spark-master 1 1 1 1 9s
deploy/trevor-spark-master 1 1 1 1 10s
deploy/trevor-spark-worker 5 5 5 5 9s
NAME DESIRED CURRENT READY AGE
rs/prometheus-trevor-spark-master-1148891927 1 1 1 9s
rs/trevor-spark-master-3438774284 1 1 1 9s
rs/trevor-spark-worker-705698751 5 5 5 9s
```

To get a list of spark clusters you can type "oc get spark clusters". You can also customize it with -o custom-columns=...
```bash
oc get sparkclusters -o custom-columns=NAME:metadata.name,WORKER:spec.workers,MASTER_URL:"spec.SparkMasterName",IMAGE:"spec.Image"
```
Output:

```bash
NAME  	     WORKER     	MASTER_URL    	         IMAGE
trevor     	5    	      trevor-spark-master    	radanalyticsio/openshift-spark:2.2-latest
```
