package oshinkocli

import (
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"github.com/zmhassan/sparkcluster-crd/crd"
	"fmt"
	"time"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	)



func CreateAlertManager(config *rest.Config, sparkConfig *crd.SparkCluster, createService bool) {
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())

	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"prom/alertmanager",
		"alertmanager-"+sparkConfig.Spec.SparkMasterName,
		"alertmanager-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"deployment-mode":"crd",
			"clustername": sparkConfig.Name,
			"app": "alertmanager-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "alertmanager-web",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 9093,
			},
		}}
	log.Println("Running Deployment..")
	deployment := CreatePod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created alert manager deployment %q.\n", result.GetObjectMeta().GetName())
	if createService == true {
		CreateAlertManagerService(clusterCfg, clientset)
	}
	// Logic similar to create SparkCluster.
}

func CreatePrometheus(config *rest.Config, sparkConfig *crd.SparkCluster, createService bool) {
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())

	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"prom/prometheus",
		sparkConfig.Spec.SparkMasterName,
		"prom-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "prometheus-" + sparkConfig.Spec.SparkMasterName,
			"deployment-mode":"crd",
			"clustername": sparkConfig.Name,
		}, []apiv1.EnvVar{
			{
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
		//TODO: Need to be able to inject custom rules.
	CreateConfigurationMap(config, sparkConfig, clusterCfg.MasterSvcURI, sparkConfig.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	log.Println("Running Deployment..")
	deployment := CreatePromPod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created prometheus deployment %q.\n", result.GetObjectMeta().GetName())
	if createService == true {
		CreateServiceObject(clusterCfg, clientset , "prometheus-", "prometheus", 9090 )
		//CreatePrometheusService(clusterCfg, clientset)
	}
	// Logic similar to create SparkCluster.
}

func CreateConfigurationMap(config *rest.Config, sparkConfig *crd.SparkCluster, key string, value string) {
	clientset := GetClientSet(config)
	cfg := &v1.ConfigMap{}
	cfg.SetName(key)
	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods(oshinkoconfig.GetNameSpace()).List(metav1.ListOptions{ LabelSelector:"clustername="+sparkConfig.Name }) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	promCfg:=GetInitPromConfig()
	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=AddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}

	cfg.Data = map[string]string{"prometheus.yml": promCfg, "simple_rule.yml": GetSimpleRule() }
	clientset.CoreV1().ConfigMaps(oshinkoconfig.GetNameSpace()).Create(cfg)
	fmt.Println("Created configmap")
}

func CreateSparkClusterObj(clusterName string, imageName string, numWorkers int, metrics string) *crd.SparkCluster {
	return &crd.SparkCluster{ObjectMeta: metav1.ObjectMeta{
		Name:   clusterName,
		Labels: map[string]string{"radanalytics": "sparkcluster"},
	},
		Spec: crd.SparkClusterSpec{
			SparkMasterName: clusterName+"-spark-master",
			SparkWorkerName: clusterName+"-spark-worker",
			Image:          imageName,
			Workers:        int32(numWorkers),
			SparkMetrics: metrics,
		},
		Status: crd.SparkClusterStatus{
			State:   "created",
			Message: "Created, not processed yet",
		},
	}
}


func CreateJob(config *rest.Config, sparkJobConfig *crd.SparkJob) {
	clientset := GetClientSet(config)
	sparkJobResult := CreateSparkJob(clientset, sparkJobConfig)
	fmt.Println("sparkMasterResult.Status: ",sparkJobResult.Status)
}
func CreateSparkJob(clientset *kubernetes.Clientset, job *crd.SparkJob) (*appsv1beta1.Deployment) {
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())

	log.Println("Running Deployment..")
	deployment := CreateSparkJobPod(job)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created deployment %q.\n", result.GetObjectMeta().GetName())

	return result

}


func CreateCluster(config *rest.Config, sparkConfig *crd.SparkCluster) {
	clientset := GetClientSet(config)
	log.Println("~~~~~~~~~~~~~~~~~~~")
	log.Println("Creating SparkCluster")
	//Deploy Spark Master
	sparkMasterResult:=CreateNewSparkMaster(clientset, sparkConfig)
	sparkWorkerResult:=CreateNewSparkWorkers(clientset, sparkConfig)
	fmt.Println("sparkMasterResult.Status: ",sparkMasterResult.Status)
	fmt.Println("sparkWorkerResult.Status: ",sparkWorkerResult.Status)

	if sparkConfig.Spec.SparkMetrics == "prometheus" {
		fmt.Println("Pausing for 15 seconds while prometheus configs get generated after pods come ready.")
		//TODO: Find a better way of knowing when a deployment is finished to run this code.
		time.Sleep(30 * time.Second)
		fmt.Println("sparkMasterResult.Status: ",sparkMasterResult.Status)
		fmt.Println("sparkWorkerResult.Status: ",sparkWorkerResult.Status)
		if sparkConfig.Spec.Alertrules !="" {
			fmt.Println("Deploying alertmanager rules "+ sparkConfig.Spec.Alertrules)
			CreateAlertManager(config, sparkConfig, true)
		}
		CreatePrometheus(config, sparkConfig, true)

	}

}


// TODO: Pass in a clusterConfig which will contain properties

type ClusterConfig struct {
	ClusterName string
	MasterSvcURI  string
	ImageName     string
	PodName       string
	ContainerName string
	ScaleNum      int32
	Labels        map[string]string
	EnvVar        []apiv1.EnvVar
	Ports         []apiv1.ContainerPort
}

func CreateNewSparkWorkers(clientset *kubernetes.Clientset, sparkConfig *crd.SparkCluster )  (*appsv1beta1.Deployment){
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		sparkConfig.Spec.Image,
		sparkConfig.Spec.SparkWorkerName,
		sparkConfig.Spec.SparkWorkerName,
		sparkConfig.Spec.Workers,
		map[string]string{
			"app": sparkConfig.Name + "-worker",
			"oshinko-cluster": sparkConfig.Name,
			"oshinko-type": "worker",
			"clustername": sparkConfig.Name,
			"deployment-mode":"crd",
 		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_ADDRESS",
				Value: "spark://" + sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7077",
			},
			{
				Name:  "SPARK_METRICS_ON",
				Value: "prometheus",
			},
			{
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



func CreateSparkJobPod(job *crd.SparkJob) *appsv1beta1.Deployment {
	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: job.Name,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: Int32Ptr(1),
			Template: apiv1.PodTemplateSpec{

				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"job": job.Name,
					} ,
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  job.Name,
							Image: job.Spec.Image,
							Env: []apiv1.EnvVar{
								{
									Name:  "SPARK_USER",
									Value: "jobrunner",
								}},
							Command: []string{"/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --class org.apache.spark.examples.SparkPi --master spark://erik-spark-master-service:7077 /opt/spark/examples/jars/spark-examples_2.11-2.2.0.jar 10"},
						},
					},
				},
			},
		},
	}
	return deployment
}

// Generic Function for pod creations
func CreatePod(config ClusterConfig) *appsv1beta1.Deployment {
	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.PodName,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: Int32Ptr(config.ScaleNum),

			Template: apiv1.PodTemplateSpec{

				ObjectMeta: metav1.ObjectMeta{
					Labels: config.Labels,
				},
				Spec: apiv1.PodSpec{
					//NodeSelector: map[string]string{ "region":"primary" },
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
			Replicas: Int32Ptr(config.ScaleNum),
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
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		sparkConfig.Spec.Image,
		sparkConfig.Spec.SparkMasterName,
		sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": sparkConfig.Spec.SparkMasterName,
			"oshinko-cluster": sparkConfig.Name,
			"oshinko-type": "master",
			"clustername": sparkConfig.Name,
			"deployment-mode":"crd",
 		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PORT",
				Value: "7077",
			},
			{
				Name:  "SPARK_MASTER_WEBUI_PORT",
				Value: "8080",
			},
			{
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
			}},
		},
	}

	service1 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterCfg.MasterSvcURI+"-ui",
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
				Name: "http",
				Port: 8080,
			}},
		},
	}

	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)

	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())

	svc_result1, svc_err1 := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service1)

	if svc_err1 != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result1.GetObjectMeta().GetName())
}

func CreateAlertManagerService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:  clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "alertmanager-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "alertmanager-web",
				Port: 9093,
			}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}

// CreateServiceObject( ? , ? , "prometheus-", "prometheus", 9090 )
// TODO: prefix == '"prometheus-"`
func CreateServiceObject(clusterCfg ClusterConfig, clientset *kubernetes.Clientset, prefix string, portname string, portNum int32){
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: prefix + clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": prefix + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: portname,
				Port: portNum,
			}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}



func Int32Ptr(i int32) *int32 { return &i }


func FindCluster(config *rest.Config, clusterName string ) (*appsv1beta1.Deployment){
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	dep, err := deploymentsClient.Get(clusterName+"-spark-master", metav1.GetOptions{})
	if err != nil {
		panic(err)
	}
		 return dep
}

func AlreadyDeployedCheck(config *rest.Config, sparkConfig *crd.SparkCluster) bool {
	clientset := GetClientSet(config)
	list, err := clientset.CoreV1().Pods(oshinkoconfig.GetNameSpace()).List(metav1.ListOptions{LabelSelector:"clustername="+sparkConfig.Name }) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}
	if len(list.Items) != 0 {
		log.Println("Spark Cluster Exists Probably due to crash ")
		return true
	} else {
		log.Println("Cluster isn't created attempting to deploy spark cluster ")
		return false
	}

}