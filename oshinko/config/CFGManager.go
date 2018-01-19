package oshinkoconfig


import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
)


func GetKubeCfg(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func GetNameSpace() string{
	NAMESPACE:=os.Getenv("CURRENT_NAMESPACE")
	return NAMESPACE
}
