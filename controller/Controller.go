package controller

import (
	"github.com/zmhassan/sparkcluster-crd/crd"
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"github.com/zmhassan/sparkcluster-crd/oshinko/oshinkocli"
	"log"
	"time"
	"github.com/zmhassan/sparkcluster-crd/client"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func StartController( config *rest.Config) {
	// Create a new clientset which include our CRD schema
	crdcs, scheme, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}
	//CreateCluster(config,nil)
	// Create a CRD client interface
	crdclient := client.CrdClient(crdcs, scheme, oshinkoconfig.GetNameSpace())
	// Watch for changes in Spark objects and fire Add, Delete, Update callbacks
	_, controller := cache.NewInformer(
		crdclient.NewListWatch(),
		&crd.SparkCluster{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				log.Printf("add: %s \n", obj)
				cls := obj.(*crd.SparkCluster)
				oshinkocli.CreateCluster(config, cls)
			},
			DeleteFunc: func(obj interface{}) {
				log.Printf("delete: %s \n", obj)
				cluster := obj.(*crd.SparkCluster)
				oshinkocli.DeleteAll(config, cluster)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				log.Printf("Update old: %s \n      New: %s\n", oldObj, newObj)
				oldCluster := oldObj.(*crd.SparkCluster)
				newCluster := newObj.(*crd.SparkCluster)
				if oldCluster.Spec.Workers != newCluster.Spec.Workers {
					oshinkocli.ScaleSparkSpark(oldCluster, newCluster, config)
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

func CreateCRDResource ( config *rest.Config)   {
	// create clientset and create our CRD, this only need to run once
	clientset, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	// note: if the CRD exist our CreateCRD function is set to exit without an error
	err =  crd.CreateCRD(clientset)
	if err != nil {
		log.Println("Msg: CRD Already Exists")
		panic(err)
	}
	// Wait for the CRD to be created before we use it (only needed if its a new one)
	time.Sleep(3 * time.Second)
	//return clientset
}
