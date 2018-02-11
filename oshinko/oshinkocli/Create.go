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


func CreateCluster(config *rest.Config, sparkConfig *crd.SparkCluster) {
	clientset := GetClientSet(config)
	log.Println("~~~~~~~~~~~~~~~~~~~")
	log.Println("Creating SparkCluster")
	//Deploy Spark Master
	sparkMasterResult:=CreateNewSparkMaster(clientset, sparkConfig)
	sparkWorkerResult:=CreateNewSparkWorkers(clientset, sparkConfig)
	fmt.Println("sparkMasterResult.Status: ",sparkMasterResult.Status)
	fmt.Println("sparkWorkerResult.Status: ",sparkWorkerResult.Status)


	if sparkConfig.Spec.Notebook == "jupyter" {
		CreateJupyterNotebook(config, sparkConfig, true)
	}
	if sparkConfig.Spec.Notebook == "zeppelin" {
		CreateZeppelinNotebook(config, sparkConfig, true)
	}
	if sparkConfig.Spec.Middleware == "jdg" {
		CreateMiddleware(config, sparkConfig, true, "jdg")
	}
	if sparkConfig.Spec.Middleware == "amq" {

		CreateMiddleware(config, sparkConfig, true, "amq")
	}
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
func CreateMiddleware(config *rest.Config, sparkConfig *crd.SparkCluster, createService bool, middleware_type string) {
	 //  jboss/infinispan-server:9.0.0.Beta1
	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())
	// TODO: Must check if middleware type is not jdg then setup amq
	//if middleware_type =="jdg"{
	//clusterCfg := CreateJDGClusterConfig(sparkConfig)

	clusterCfg := GetMiddlewareConfig(middleware_type, sparkConfig)

	if middleware_type == "jdg" {
		deployment := CreatePod(clusterCfg)
		result, err := deploymentsClient.Create(deployment)
		if err != nil {
			panic(err)
		}
		log.Printf("Created middleware deployment %q.\n", result.GetObjectMeta().GetName())
	}else if middleware_type == "amq"  {
		deployment := CreateAMQPod(clusterCfg)
		result, err := deploymentsClient.Create(deployment)
		if err != nil {
			panic(err)
		}
		log.Printf("Created middleware deployment %q.\n", result.GetObjectMeta().GetName())
	}

	if createService == true  && middleware_type == "jdg"{
		CreateServiceObject(clusterCfg, clientset , "jdg-", "jdg-hotrod-port", 11222 )
		//CreateJDGService(clusterCfg, clientset)
	}
	if createService == true  && middleware_type == "amq"{
		CreateAMQService(clusterCfg, clientset)
	}

}
func GetMiddlewareConfig(middleware_type string, sparkConfig *crd.SparkCluster) ClusterConfig {
	if middleware_type == "jdg" {
		return  CreateJDGClusterConfig(sparkConfig)
	} else if middleware_type == "amq" {
		return CreateAMQConfig(sparkConfig)
	}else {
		return ClusterConfig{}
	}
}
func CreateAMQConfig(sparkConfig *crd.SparkCluster) ClusterConfig {
	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"redhatiot/artemis:latest",
		"amq-" +sparkConfig.Spec.SparkMasterName + "-middleware",
		"amq-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "amq-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "amq-amqp-port",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 5672,
			},
			{
				Name:          "amq-web-port",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 8161,
			},
			{
				Name:          "amq-mqtt-port",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 1883,
			},
		}}
		return clusterCfg
}
func CreateJDGClusterConfig(sparkConfig *crd.SparkCluster) ClusterConfig {
	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"jboss/infinispan-server:9.0.0.Beta1",
		"jdg-"+sparkConfig.Spec.SparkMasterName + "-middleware",
		"jdg-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "jdg-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "jdg-hotrod-port",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 11222,
			},
		}}
	return clusterCfg
}
func CreateZeppelinNotebook(config *rest.Config, sparkConfig *crd.SparkCluster, createService bool) {

	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())

	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"rimolive/zeppelin-openshift",
		sparkConfig.Spec.SparkMasterName+"-notebook",
		"zeppelin-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "zeppelin-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "zeppelin-notebook",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 8080,
			},
		}}
	//CreateConfigurationMap(config, sparkConfig, clusterCfg.MasterSvcURI, sparkConfig.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	log.Println("Running Deployment..")
	deployment := CreatePod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created prometheus deployment %q.\n", result.GetObjectMeta().GetName())
	if createService == true {
		CreateZeppelinService(clusterCfg, clientset)
	}

}



func CreateJupyterNotebook(config *rest.Config, sparkConfig *crd.SparkCluster, createService bool) {

	clientset := GetClientSet(config)
	deploymentsClient := clientset.AppsV1beta1().Deployments(oshinkoconfig.GetNameSpace())

	clusterCfg := ClusterConfig{
		sparkConfig.Name,
		sparkConfig.Spec.SparkMasterName + SRV_SUFFIX,
		"radanalyticsio/base-notebook",
		sparkConfig.Spec.SparkMasterName+"-notebook",
		"prom-" + sparkConfig.Spec.SparkMasterName,
		1,
		map[string]string{
			"app": "jupyter-" + sparkConfig.Spec.SparkMasterName,
		}, []apiv1.EnvVar{
			{
				Name:  "SPARK_MASTER_PROM_URI",
				Value: sparkConfig.Spec.SparkMasterName + SRV_SUFFIX + ":7777",
			},
		}, []apiv1.ContainerPort{
			{
				Name:          "jupyter-notebook",
				Protocol:      apiv1.ProtocolTCP,
				ContainerPort: 8888,
			},
		}}
	//CreateConfigurationMap(config, sparkConfig, clusterCfg.MasterSvcURI, sparkConfig.Spec.SparkMasterName+SRV_SUFFIX+":7777")
	log.Println("Running Deployment..")
	deployment := CreatePod(clusterCfg)
	result, err := deploymentsClient.Create(deployment)
	if err != nil {
		panic(err)
	}
	log.Printf("Created prometheus deployment %q.\n", result.GetObjectMeta().GetName())
	if createService == true {
		CreateJupyterService(clusterCfg, clientset)
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
			"clustername": sparkConfig.Name,
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


func CreateAMQPod(config ClusterConfig) *appsv1beta1.Deployment {

	deployment := &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "amq-" + config.PodName,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: Int32Ptr(config.ScaleNum),
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: config.Labels,
				},
				Spec: apiv1.PodSpec{
					Hostname:"amq-" + config.PodName,
					Containers: []apiv1.Container{
						{
							Name:  config.ContainerName,
							Image: config.ImageName,
							Env:   config.EnvVar,
							Command: []string{"/opt/apache-artemis-1.4.0/bin/run_artemis.sh"},
							VolumeMounts: []apiv1.VolumeMount{
								{	Name: "vol-instance",
									MountPath: "/var/run/artemis",
								},
							},
						},
					},

					Volumes: []apiv1.Volume{
						{
							Name: "vol-instance",
							VolumeSource: apiv1.VolumeSource{
								//PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ ClaimName: "prom-storage", ReadOnly:  false,},
								EmptyDir: &apiv1.EmptyDirVolumeSource{},
							},

						},
					},
				},
			},
		},
	}
	return deployment
}


// TODO: Create AMQ pod with the command run:  /opt/apache-artemis-1.4.0/bin/run_artemis.sh
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
			"clustername": sparkConfig.Name,
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
			}, {
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
	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}


func CreateAMQService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "amq-" + clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "amq-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
					Name: "amq-web-port",
					Port: 8161,
				},{
					Name: "amq-amqp-port",
					Port: 5672,
				},
				{
					Name: "amq-mqtt-port",
					Port: 1883,
				}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {
		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}
//
func CreateJDGService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:  clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "jdg-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "jdg-hotrod-port",
				Port: 11222,
			}},
		},
	}
 	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {
		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}

func CreateZeppelinService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zeppelin-" + clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "zeppelin-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "zeppelin-web",
				Port: 8080,
			}},
		},
	}
	svc_result, svc_err := clientset.CoreV1().Services(oshinkoconfig.GetNameSpace()).Create(service)
	if svc_err != nil {

		panic(svc_err)
	}
	log.Printf("Created Service %q.\n", svc_result.GetObjectMeta().GetName())
}

func CreateJupyterService(clusterCfg ClusterConfig, clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "jupyter-" + clusterCfg.PodName + SRV_SUFFIX,
			Labels: map[string]string{
				"app": "jupyter-" + clusterCfg.MasterSvcURI + SRV_SUFFIX,
			},
			OwnerReferences: []metav1.OwnerReference{},
		},
		Spec: v1.ServiceSpec{
			Type:      "ClusterIP",
			ClusterIP: "None",
			Selector:  clusterCfg.Labels,
			Ports: []v1.ServicePort{{
				Name: "jupyter-web",
				Port: 8888,
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