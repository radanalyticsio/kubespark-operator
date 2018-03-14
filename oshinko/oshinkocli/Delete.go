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
	// Deleting spark cluster and service for spark master
	DeleteDeployment(config, cluster.Spec.SparkMasterName)
	DeleteDeployment(config, cluster.Spec.SparkWorkerName)
	DeleteService(config, cluster.Spec.SparkMasterName+SRV_SUFFIX)
	// Deleting Prometheus
	if cluster.Spec.SparkMetrics == "prometheus"{

		DeleteDeployment(config, "prometheus-"+cluster.Spec.SparkMasterName)
		DeleteService(config, "prometheus-"+cluster.Spec.SparkMasterName+SRV_SUFFIX)
		DeleteDeployment(config, "alertmanager-"+cluster.Spec.SparkMasterName)
		DeleteService(config, "alertmanager-"+cluster.Spec.SparkMasterName+SRV_SUFFIX)

	}
	DeleteConfigMap(config, cluster)
}

func DeleteConfigMap(config *rest.Config, sparkCluster *crd.SparkCluster) {
	clientset := GetClientSet(config)
	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods(oshinkoconfig.GetNameSpace()).List(metav1.ListOptions{LabelSelector: "clustername=" + sparkCluster.Name}) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	promCfg := GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg += AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}
	deletePolicy := metav1.DeletePropagationForeground
	cerr := clientset.CoreV1().ConfigMaps(oshinkoconfig.GetNameSpace()).Delete(sparkCluster.Spec.SparkMasterName+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if cerr != nil {
		panic(err)
	}
	fmt.Println("Deleted configmap")
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
