package oshinkocli

import (
	"github.com/zmhassan/sparkcluster-crd/crd"
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"fmt"
	"log"

	"k8s.io/client-go/rest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const SRV_SUFFIX = "-service"

//Wrapper - For deleting All
func DeleteAll(config *rest.Config, cluster *crd.SparkCluster) {
	DeleteSparkCluster(config, cluster.Spec.SparkMasterName, cluster.Spec.SparkWorkerName)
	DeleteSparkClusterService(config, cluster.Spec.SparkMasterName)
	DeletePrometheusDeployment(config, cluster.Spec.SparkMasterName)
	DeletePrometheusService(config, cluster.Spec.SparkMasterName)

	if cluster.Spec.Notebook =="jupyter"{
		//DeleteJupyterService(config, cluster.Spec.SparkMasterName)
		DeleteDeployment(config,cluster.Spec.SparkMasterName+"-notebook" )
		DeleteService(config, "jupyter-"+cluster.Spec.SparkMasterName+"-notebook"+SRV_SUFFIX )
	}
	if cluster.Spec.Notebook == "zeppelin"{
		DeleteDeployment(config,cluster.Spec.SparkMasterName+"-notebook" )
		DeleteService(config, "zeppelin-"+cluster.Spec.SparkMasterName+"-notebook"+SRV_SUFFIX )
	}

	DeleteConfigMap(config, cluster)
}


func DeleteConfigMap(config *rest.Config, sparkCluster *crd.SparkCluster) {
	clientset := GetClientSet(config)
	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods(oshinkoconfig.GetNameSpace()).List(metav1.ListOptions{LabelSelector:"clustername="+sparkCluster.Name }) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	promCfg:=GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}
	deletePolicy := metav1.DeletePropagationForeground
	cerr:=clientset.CoreV1().ConfigMaps(oshinkoconfig.GetNameSpace()).Delete(sparkCluster.Spec.SparkMasterName+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if cerr != nil {
		panic(err)
	}
	fmt.Println("Deleted configmap")
}


func DeletePrometheusService(config *rest.Config, sparkmastername string) {
	clientset := GetClientSet(config)
	deletePolicy := metav1.DeletePropagationForeground
	svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Delete("prometheus-"+sparkmastername+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if svc_err != nil {
		panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", sparkmastername+SRV_SUFFIX)
}

func DeleteJupyterService(config *rest.Config, sparkmastername string) {
	clientset := GetClientSet(config)
	deletePolicy := metav1.DeletePropagationForeground
	//jupyter-hk-spark-master-notebook-service
	svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Delete("jupyter-"+sparkmastername+"-notebook"+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if svc_err != nil {
		panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", sparkmastername+SRV_SUFFIX)
}

func DeleteService(config *rest.Config, servicename string) {


	clientset := GetClientSet(config)
	deletePolicy := metav1.DeletePropagationForeground
	//jupyter-hk-spark-master-notebook-service
	svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Delete(servicename, &metav1.DeleteOptions{
	PropagationPolicy: &deletePolicy,
	})
	if svc_err != nil {
	panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", servicename)



}


func DeleteSparkClusterService(config *rest.Config, sparkmastername string) {
	clientset := GetClientSet(config)
	deletePolicy := metav1.DeletePropagationForeground
	svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Delete(sparkmastername+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})

	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", sparkmastername+SRV_SUFFIX)

}




func DeleteDeployment(config *rest.Config, name string) {
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	deletePolicy := metav1.DeletePropagationForeground
	if err := deploymentsClient.Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
	}

	log.Println("Deleted nodes")
}

func DeletePrometheusDeployment(config *rest.Config, masterName string) {
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	deletePolicy := metav1.DeletePropagationForeground
	if err := deploymentsClient.Delete("prometheus-"+masterName, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
	}

	log.Println("Deleted nodes")
}

func DeleteSparkCluster(config *rest.Config, masterName string, workerName string) {
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	deletePolicy := metav1.DeletePropagationForeground
	if err := deploymentsClient.Delete(masterName, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
	}

	deletePolicyW := metav1.DeletePropagationForeground
	if errw := deploymentsClient.Delete(workerName, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicyW,
	}); errw != nil {
		panic(errw)
	}

	log.Println("Deleted nodes")
}

