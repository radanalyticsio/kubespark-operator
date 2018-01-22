package main


import (
	"fmt"
	"time"
	"github.com/zmhassan/sparkcluster-crd/client"
	"github.com/zmhassan/sparkcluster-crd/crd"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"flag"
	"os"
)

// return rest config, if path not specified assume in cluster config
func GetClientCfg(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func main(){
	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()
	config, err := GetClientCfg(*kubeconf)
	if err != nil {
		panic(err.Error())
	}
	// create clientset and create our CRD, this only need to run once
	clientset, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	createCRD( clientset)
	sparkCluster := createSparkClusterObj("spark-master","spark-worker","test-add",
		"radanalyticsio/openshift-spark:2.2-latest",
		3, "prometheus")

	deployCluster(config, sparkCluster, "myproject")
	fmt.Println("Done Deployment ")
	//fmt.Println("Msg: Listing out objects")
	//// List all Example objects
	//items, err := crdclient.List(meta_v1.ListOptions{})
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("List:\n%s\n", items)
}
func createCRD( clientset *apiextcs.Clientset) {
	// note: if the CRD exist our CreateCRD function is set to exit without an error
	err := crd.CreateCRD(clientset)
	if err != nil {
		fmt.Println("Msg: CRD Already Exists")
		panic(err)
	}
	// Wait for the CRD to be created before we use it (only needed if its a new one)
	time.Sleep(3 * time.Second)
}

func deployCluster( config *rest.Config, sparkCluster *crd.SparkCluster, ns string) {
	crdcs, scheme, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}
	crdclient := client.CrdClient(crdcs, scheme, ns)
	result, err := crdclient.Create(sparkCluster)
	if err == nil {
		fmt.Printf("CREATED: %#v\n", result)
	} else if apierrors.IsAlreadyExists(err) {
		fmt.Printf("ALREADY EXISTS: %#v\n", result)
	} else {
		panic(err)
	}
}

func createSparkClusterObj(sparkmastername string, sparkworkername string, clusterName string, imageName string, numWorkers int, metrics string) *crd.SparkCluster {
	return &crd.SparkCluster{ObjectMeta: meta_v1.ObjectMeta{
		Name:   clusterName,
		Labels: map[string]string{"radanalytics": "sparkcluster"},
	},
		Spec: crd.SparkClusterSpec{
			SparkMasterName: sparkmastername,
			SparkWorkerName: sparkworkername,
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

