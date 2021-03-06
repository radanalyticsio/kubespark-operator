package main

import (
	"flag"
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"github.com/zmhassan/sparkcluster-crd/controller"
	"os"
	"io/ioutil"
	"log"
)



// Main entry point of application. It will create a CRD and create the controller/operator that will manage the crd in kubernetes.
func main() {
	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")
	//kubeconf := flag.String("kubeconf","", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()
	config, err := oshinkoconfig.GetKubeCfg(*kubeconf)
	if err != nil {
		panic(err.Error())
	}

	WelcomeMsgSparkJob("0.0.1-SNAPSHOT")
	controller.CreateSparkJobCRDResource(config)
	controller.StartSparkJobController(config)

}


func WelcomeMsgSparkJob(version string) {
	data, err := ioutil.ReadFile("Banner.txt")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	log.Println(string(data))
	log.Println("Component: ", " Spark Job Operator")
	log.Println("Version: ", version)
}

