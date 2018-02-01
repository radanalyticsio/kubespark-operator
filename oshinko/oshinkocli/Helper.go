package oshinkocli
import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
)

func GetClientSet(config *rest.Config) (*kubernetes.Clientset ) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	return clientset
}
