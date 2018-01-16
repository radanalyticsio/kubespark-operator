package main

import (
	"flag"
	"fmt"
	"os"

	v1api "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
	config, err := GetClientCfg(*kubeconf)
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	cfg := &v1api.ConfigMap{}
	cfg.SetName("prom-config2")

	fmt.Println("Config" + XGetPromConfig("trevor", "trevor-spark-master-service"))
	cfg.Data = map[string]string{"prometheus.yml": XGetPromConfig("trevor", "trevor-spark-master-service")}
	clientset.CoreV1().ConfigMaps("myproject").Create(cfg)
	fmt.Println("Created configmap")
}

func XGetTopPromCfg() string {
	return `
    global:
      scrape_interval:     5s
      evaluation_interval: 5s
    scrape_configs:
  `
}

func XGetPromConfig(clusterName string, sparkmaster string) string {
	promcfg := GetTopPromCfg()
	promcfg += fmt.Sprintf(
		`
      - job_name: '%s'
        static_configs:
          - targets: ['%s:7777']
`, clusterName, sparkmaster)
	return promcfg
}
