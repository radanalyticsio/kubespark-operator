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
	"fmt"
	"time"
	"github.com/zmhassan/sparkcluster-crd/client"
	"github.com/zmhassan/sparkcluster-crd/crd"
	"github.com/zmhassan/sparkcluster-crd/authentication"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"flag"
	"github.com/radanalyticsio/oshinko-cli/core/clusters"

	"os"
	"io"
)

// return rest config, if path not specified assume in cluster config
func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func main() {

	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")
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
		fmt.Println("Msg: CRD Already Exists")
		panic(err)
	}

	// Wait for the CRD to be created before we use it (only needed if its a new one)
	time.Sleep(3 * time.Second)

	// Create a new clientset which include our CRD schema
	crdcs, scheme, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}

	// Create a CRD client interface
	crdclient := client.CrdClient(crdcs, scheme, "default")


	// Watch for changes in Spark objects and fire Add, Delete, Update callbacks
	_, controller := cache.NewInformer(
		crdclient.NewListWatch(),
		&crd.SparkCluster{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				fmt.Printf("add: %s \n", obj)
				cls:=obj.(*crd.SparkCluster)
				config := clusters.ClusterConfig{}
				oclient, _:=authentication.GetOpenShiftClient()
				kclient, _:=authentication.GetKubeClient()

				clusters.CreateCluster("sparkit", "default", "radanalyticsio/openshift-spark:2.2-latest",
					&config, oclient , kclient , "sparkit", false)

				fmt.Println("Image is: ", cls.Spec.Image)
				fmt.Println("Workers is: ", cls.Spec.Workers)
				fmt.Println("SparkMetricsON is: ", cls.Spec.SparkMetricsOn)
				//TODO: Create logic here that will create the necessary pods that would compose of a spark cluster.

			},
			DeleteFunc: func(obj interface{}) {
				fmt.Printf("delete: %s \n", obj)

			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				fmt.Printf("Update old: %s \n      New: %s\n", oldObj, newObj)
			},
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)

	// Wait forever
	select {}
}
