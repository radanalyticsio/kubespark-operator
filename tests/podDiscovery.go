package main

import (
	"fmt"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"flag"
	"os"

)

// return rest config, if path not specified assume in cluster config
func XGetClientCfg(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func main() {
	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()
	config, err := XGetClientCfg(*kubeconf)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	//TODO Discover pods with a particular label: sparkcluster=trevor
	list, err := clientset.CoreV1().Pods("myproject").List(meta_v1.ListOptions{ LabelSelector:"clustername=erik" }) //LabelSelector:"sparkcluster=trevor"})
	if err != nil {
		panic(err)
	}

	promCfg:=XGetInitPromConfig()

	for _, d := range list.Items {
		fmt.Println(d.Name, d.Status.PodIP+":7777")
		promCfg+=XAddSparkNodeToMonitor(d.Name, d.Status.PodIP+":7777")
	}
	fmt.Println(promCfg)
	//TODO: Create Configmap using master-service-name as the key

	//CreateConfigurationMap(nil, nil,"key",promCfg)

}
//func CreateConfigurationMap(config *rest.Config, sparkConfig *crd.SparkCluster, key string, value string) {
//
//	clientset, err := kubernetes.NewForConfig(config)
//	if err != nil {
//		panic(err)
//	}
//
//	cfg := &v1.ConfigMap{}
//	cfg.SetName(key)
//
//	fmt.Println("Config" + GetPromConfig(key, value))
//	cfg.Data = map[string]string{"prometheus.yml": GetPromConfig(key, value)}
//	clientset.CoreV1().ConfigMaps(GetNameSpace()).Create(cfg)
//	fmt.Println("Created configmap")
//}

func XGetInitPromConfig() string {
	return `
    global:
      scrape_interval:     5s
      evaluation_interval: 5s
    scrape_configs:
  `
}

func XAddSparkNodeToMonitor(hostname string, target string) string {


	promcfg := fmt.Sprintf(
		`
      - job_name: '%s'
        static_configs:
          - targets: ['%s']
`, hostname, target)
	return promcfg
}
