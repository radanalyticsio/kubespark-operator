package oshinkocli

import (
	"github.com/zmhassan/sparkcluster-crd/crd"
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"fmt"
	"time"
	"log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

)

func ScaleSparkSpark(oldCluster *crd.SparkCluster, newCluster *crd.SparkCluster, config *rest.Config) {
	//log.Println("Scaling  cluster from: ", oldCluster.Spec.Workers)
	log.Println("Scaling  cluster to: ", newCluster.Spec.Workers)
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	//TODO: Bug here metav1.GetOptions{} is gonna look for all spark clusters not just the one we are working with.
	deps, err := deploymentsClient.Get(newCluster.Spec.SparkWorkerName, metav1.GetOptions{})
 	deps.Spec.Replicas = Int32Ptr(newCluster.Spec.Workers)
	result, err := deploymentsClient.Update(deps)
	if err != nil {
		panic(err)
	}
	fmt.Println("Waiting for 1 minute while it is running the scaledown/up process")
	time.Sleep(30 * time.Second)
	UpdateConfigurationMap(config, newCluster, newCluster.Spec.SparkMasterName + SRV_SUFFIX, newCluster.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	UpdatePrometheusDeployment(config, newCluster.Spec.SparkMasterName, newCluster)
	log.Printf("Scaled deployment complete: %q.\n", result.GetObjectMeta().GetName())
}



// TODO: Figure out a way to roll out new prometheus when users scale up or down.
func UpdatePrometheusDeployment(config *rest.Config, masterName string, sparkConfig *crd.SparkCluster) {
 	fmt.Println("Undeploying prometheus")
	DeletePrometheusDeployment(config,masterName)
	fmt.Println("Waiting 30 seconds")
	time.Sleep(10* time.Second)
	fmt.Println("Deploying prometheus")
	CreatePrometheus(config,sparkConfig,false)
	log.Println("Done prometheus")
}

//TODO: Remove unnecessary 'value' argument as its never used.
func UpdateConfigurationMap(config *rest.Config, sparkConfig *crd.SparkCluster, key string, value string){
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	// TODO: Make 'clustername=' into a const.
	list, err := clientset.CoreV1().Pods(oshinkoconfig.GetNameSpace()).List(metav1.ListOptions{LabelSelector:"clustername=" + sparkConfig.Name })
	if err != nil {
		panic(err)
	}
	promCfg:=GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}
	cfgMap,cerr:=clientset.CoreV1().ConfigMaps(oshinkoconfig.GetNameSpace()).Get(key,metav1.GetOptions{})
	if cerr != nil {
		panic(err)
	}
	cfgMap.Data = map[string]string{"prometheus.yml": promCfg}
	clientset.CoreV1().ConfigMaps(oshinkoconfig.GetNameSpace()).Update(cfgMap)
	fmt.Println("Created configmap")
}
