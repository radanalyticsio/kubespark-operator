/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/zmhassan/sparkcluster-crd/client"
	"github.com/zmhassan/sparkcluster-crd/crd"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	apiv1 "k8s.io/api/core/v1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"os"
)


const SRV_SUFFIX = "-service"

// return rest config, if path not specified assume in cluster config
func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func GetNameSpace() string{
	NAMESPACE:=os.Getenv("CURRENT_NAMESPACE")
	return NAMESPACE
}
func main() {

	rest.InClusterConfig()

	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")
	//kubeconf := flag.String("kubeconf","", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()

	config, err := GetClientConfig(*kubeconf)
	if err != nil {
		panic(err.Error())
	}

	// create clientset and create our CRD, this only need to run once
	clientset, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// note: if the CRD exist our CreateCRD function is set to exit without an error
	err = crd.CreateCRD(clientset)
	if err != nil {
		log.Println("Msg: CRD Already Exists")
		panic(err)
	}

	// Wait for the CRD to be created before we use it (only needed if its a new one)
	time.Sleep(3 * time.Second)

	// Create a new clientset which include our CRD schema
	crdcs, scheme, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}

	//CreateCluster(config,nil)

	// Create a CRD client interface
	crdclient := client.CrdClient(crdcs, scheme, GetNameSpace())

	// Watch for changes in Spark objects and fire Add, Delete, Update callbacks
	_, controller := cache.NewInformer(
		crdclient.NewListWatch(),
		&crd.SparkCluster{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				log.Printf("add: %s \n", obj)
				cls := obj.(*crd.SparkCluster)

				//CreateCluster()

				CreateCluster(config, cls)

				log.Println("Image is: ", cls.Spec.Image)
				log.Println("Workers is: ", cls.Spec.Workers)
				log.Println("SparkMetricsON is: ", cls.Spec.SparkMetrics)


			},
			DeleteFunc: func(obj interface{}) {
				log.Printf("delete: %s \n", obj)
				cluster := obj.(*crd.SparkCluster)
				DeleteSparkCluster(config, cluster.Spec.SparkMasterName, cluster.Spec.SparkWorkerName)
				DeleteSparkClusterService(config, cluster.Spec.SparkMasterName)
				DeletePrometheusDeployment(config, cluster.Spec.SparkMasterName)
				DeletePrometheusService(config, cluster.Spec.SparkMasterName)
				//TODO: Delete ConfigMap

			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				log.Printf("Update old: %s \n      New: %s\n", oldObj, newObj)
				oldCluster := oldObj.(*crd.SparkCluster)
				newCluster := newObj.(*crd.SparkCluster)
				if oldCluster.Spec.Workers != newCluster.Spec.Workers {
					ScaleSparkSpark(oldCluster,newCluster,config)
					//(config, cluster.Spec
				}
			},
		},
	)

	log.Println("Starting controller")
	stop := make(chan struct{})
	go controller.Run(stop)

	// Wait forever
	select {}
}
func ScaleSparkSpark(oldCluster *crd.SparkCluster, newCluster *crd.SparkCluster, config *rest.Config) {
	log.Println("Scaling  cluster from: ", oldCluster.Spec.Workers)
	log.Println("Scaling  cluster to: ", newCluster.Spec.Workers)
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}


	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())

	deps, err := deploymentsClient.Get(newCluster.Spec.SparkWorkerName, metav1.GetOptions{})
	//num:=deps.Spec.Replicas
	deps.Spec.Replicas = int32Ptr(newCluster.Spec.Workers)
	result, err := deploymentsClient.Update(deps)
	if err != nil {
		panic(err)
	}
	fmt.Println("Waiting for 1 minute while it is running the scaledown/up process")
	time.Sleep(1 * time.Minute)
	UpdateConfigurationMap(config, newCluster, newCluster.Spec.SparkMasterName + SRV_SUFFIX, newCluster.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	UpdatePrometheusDeployment(config, newCluster.Spec.SparkMasterName)
	log.Printf("Scaled deployment complete: %q.\n", result.GetObjectMeta().GetName())
}
// TODO: Figure out a way to roll out new prometheus when users scale up or down.
func UpdatePrometheusDeployment(config *rest.Config, masterName string) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Scaling prometheus to 1")
	deployments,derr := clientset.AppsV1beta1().Deployments(GetNameSpace()).Get("prometheus-"+masterName, metav1.GetOptions{})
	deployments.Spec.Replicas=int32Ptr(0)

	if derr != nil {
		panic(err)
	}
	clientset.AppsV1beta1().Deployments(GetNameSpace()).Update(deployments)
	fmt.Println("Waiting 30 seconds")
	time.Sleep(30* time.Second)
	fmt.Println("Scaling prometheus to 1")
	deployments.Spec.Replicas=int32Ptr(1)
	clientset.AppsV1beta1().Deployments(GetNameSpace()).Update(deployments)
	log.Println("Updated prometheus")
}


func DeletePrometheusService(config *rest.Config, sparkmastername string) {

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	deletePolicy := metav1.DeletePropagationForeground
	svc_err := clientset.CoreV1().Services(GetNameSpace()).Delete("prometheus-"+sparkmastername+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})

	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", sparkmastername+SRV_SUFFIX)
}
func CreatePrometheus(config *rest.Config, sparkConfig *crd.SparkCluster) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())

	clusterCfg := ClusterConfig{
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"prom/prometheus",
		sparkConfig.Spec.SparkMasterName,
		"prom-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "prometheus-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			apiv1.EnvVar{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "prometheus-web",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 9090,
			},
		}}
	CreateConfigurationMap(config, sparkConfig, clusterCfg.MasterSvcURI, sparkConfig.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	log.Println("Running Deployment..")
	deployment := CreatePromPod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created prometheus deployment %q.\n", result.GetObjectMeta().GetName())
	CreatePrometheusService(clusterCfg, clientset)
	// Logic similar to create SparkCluster.
}

func UpdateConfigurationMap(config *rest.Config, sparkConfig *crd.SparkCluster, key string, value string){
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods(GetNameSpace()).List(metav1.ListOptions{}) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	promCfg:=GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}
	cfgMap,cerr:=clientset.CoreV1().ConfigMaps(GetNameSpace()).Get(key,metav1.GetOptions{})
	if cerr != nil {
		panic(err)
	}
	cfgMap.Data = map[string]string{"prometheus.yml": promCfg}
	clientset.CoreV1().ConfigMaps(GetNameSpace()).Update(cfgMap)
	fmt.Println("Created configmap")
}
func CreateConfigurationMap(config *rest.Config, sparkConfig *crd.SparkCluster, key string, value string) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	cfg := &v1.ConfigMap{}
	cfg.SetName(key)
	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods(GetNameSpace()).List(metav1.ListOptions{}) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	promCfg:=GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}

	cfg.Data = map[string]string{"prometheus.yml": promCfg}
	clientset.CoreV1().ConfigMaps(GetNameSpace()).Create(cfg)
	fmt.Println("Created configmap")
}

func GetInitPromConfig() string {
	return `
    global:
      scrape_interval:     5s
      evaluation_interval: 5s
    scrape_configs:
  `
}

func AddSparkNodeToMonitor(hostname string, target string) string {
	return fmt.Sprintf(
		`
      - job_name: '%s'
        static_configs:
          - targets: ['%s']
`, hostname, target)

}


func DeleteSparkClusterService(config *rest.Config, sparkmastername string) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	deletePolicy := metav1.DeletePropagationForeground
	svc_err := clientset.CoreV1().Services(GetNameSpace()).Delete(sparkmastername+SRV_SUFFIX, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})

	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Deleted Service %q.\n", sparkmastername+SRV_SUFFIX)

}
func CreateCluster(config *rest.Config, sparkConfig *crd.SparkCluster) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	log.Println("~~~~~~~~~~~~~~~~~~~")
	log.Println("Creating SparkCluster")
	//Deploy Spark Master

	sparkMasterResult:=CreateNewSparkMaster(clientset, sparkConfig)
	sparkWorkerResult:=CreateNewSparkWorkers(clientset, sparkConfig)
	fmt.Println("sparkMasterResult.Status: ",sparkMasterResult.Status)
	fmt.Println("sparkWorkerResult.Status: ",sparkWorkerResult.Status)


	if sparkConfig.Spec.SparkMetrics == "prometheus" {
		fmt.Println("Pausing for 1 min while prometheus configs get generated after pods come ready.")
		time.Sleep(1 * time.Minute)


		fmt.Println("sparkMasterResult.Status: ",sparkMasterResult.Status)
		fmt.Println("sparkWorkerResult.Status: ",sparkWorkerResult.Status)
		CreatePrometheus(config, sparkConfig)
	}

}

// TODO: Pass in a clusterConfig which will contain properties

type ClusterConfig struct {
	MasterSvcURI  string
	ImageName     string
	PodName       string
	ContainerName string
	ScaleNum      int32
	Labels        map[string]string
	EnvVar        []apiv1.EnvVar
	Ports         []apiv1.ContainerPort
}

func DeletePrometheusDeployment(config *rest.Config, masterName string) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())
	deletePolicy := metav1.DeletePropagationForeground
	if err := deploymentsClient.Delete("prometheus-"+masterName, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
	}

	log.Println("Deleted nodes")
}

func DeleteSparkCluster(config *rest.Config, masterName string, workerName string) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())

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

func CreateNewSparkWorkers(clientset *kubernetes.Clientset, sparkConfig *crd.SparkCluster)  (*appsv1beta1.Deployment){
	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())
	clusterCfg := ClusterConfig{
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		sparkConfig.Spec.Image,
		sparkConfig.Spec.SparkWorkerName,
		sparkConfig.Spec.SparkWorkerName,
		sparkConfig.Spec.Workers,
		map[string]string{
			"app": sparkConfig.Name + "-worker",
			"clustername": sparkConfig.GetObjectMeta().GetClusterName(),
		}, []apiv1.EnvVar{
			apiv1.EnvVar{
				Name:  "SPARK_MASTER_ADDRESS",
				Value: "spark://" + sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7077",
			},
			apiv1.EnvVar{
				Name:  "SPARK_METRICS_ON",
				Value: "prometheus",
			},
			apiv1.EnvVar{
				Name:  "SPARK_MASTER_UI_ADDRESS",
				Value: "http://" + sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":8080",
			}}, nil}
	deployment := CreatePod(clusterCfg)
	log.Println("Running Deployment..")
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created deployment %q.\n", result.GetObjectMeta().GetName())

	return result
}

// Generic Function for pod creations
func CreatePod(config ClusterConfig) *appsv1beta1.Deployment {
	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.PodName,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: int32Ptr(config.ScaleNum),
			Template: apiv1.PodTemplateSpec{

				ObjectMeta: metav1.ObjectMeta{
					Labels: config.Labels,
				},
				Spec: apiv1.PodSpec{
					Hostname: config.PodName,
					Containers: []apiv1.Container{
						{
							Name:  config.ContainerName,
							Image: config.ImageName,
							Env:   config.EnvVar,
						},
					},
				},
			},
		},
	}
	return deployment
}

func CreatePromPod(config ClusterConfig) *appsv1beta1.Deployment {

	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "prometheus-" + config.PodName,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: int32Ptr(config.ScaleNum),
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: config.Labels,
				},
				Spec: apiv1.PodSpec{
					Hostname:"prometheus-" + config.PodName,
					Containers: []apiv1.Container{
						{
							Name:  config.ContainerName,
							Image: config.ImageName,
							Env:   config.EnvVar,
							VolumeMounts: []apiv1.VolumeMount{
								{	Name: "prometheus-store",
									MountPath: "/prometheus/",
								},
								{	Name: config.MasterSvcURI,
									MountPath:"/etc/prometheus",
								},
							},
						},
					},
					Volumes: []apiv1.Volume{
						{
							Name: "prometheus-store",
							VolumeSource: apiv1.VolumeSource{
								//PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ ClaimName: "prom-storage", ReadOnly:  false,},
							EmptyDir: &apiv1.EmptyDirVolumeSource{},
							},

						}, { Name: config.MasterSvcURI,
							 VolumeSource: apiv1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name:config.MasterSvcURI},
								},
							},},
					},
				},
			},
		},
	}
	return deployment
}

func CreateNewSparkMaster(clientset *kubernetes.Clientset, sparkConfig *crd.SparkCluster)  (*appsv1beta1.Deployment){
	deploymentsClient := clientset.AppsV1beta1().Deployments(GetNameSpace())
	clusterCfg := ClusterConfig{
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"radanalyticsio/openshift-spark",
		sparkConfig.Spec.SparkMasterName,
		sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": sparkConfig.Spec.SparkMasterName,
			"clustername": sparkConfig.GetObjectMeta().GetClusterName(),
		}, []apiv1.EnvVar{
			apiv1.EnvVar{
				Name:  "SPARK_MASTER_PORT",
				Value: "7077",
			},
			apiv1.EnvVar{
				Name:  "SPARK_MASTER_WEBUI_PORT",
				Value: "8080",
			},
			apiv1.EnvVar{
				Name:  "SPARK_METRICS_ON",
				Value: "prometheus",
			}}, []apiv1.ContainerPort{
			{
				Name:          "sparksubmit",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 7077,
			},
			{
				Name:          "prometheus",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 7777,
			},
			{
				Name:          "http",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 8080,
			},
		}}
	log.Println("Running Deployment..")
	deployment := CreatePod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created deployment %q.\n", result.GetObjectMeta().GetName())
	CreateSparkClusterService(clusterCfg, clientset)
	return result

}
func CreateSparkClusterService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterCfg.MasterSvcURI,
			Labels: map[string]string{
				"app": "spark",
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "sparksubmit",
				Port: 7077,
			}, {
				Name: "prometheus",
				Port: 7777,
			}, {
				Name: "http",
				Port: 8080,
			}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(GetNameSpace()).Create(service)

	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}

func CreatePrometheusService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "prometheus-" + clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "prometheus-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "prometheus",
				Port: 9090,
			}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(GetNameSpace()).Create(service)
	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}

func int32Ptr(i int32) *int32 { return &i }
