package crd

import (
	"reflect"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
)

const (
	CRDPlural   = "sparkclusters"
	CRDGroup    = "radanalytics.redhat.com"
	CRDVersion  = "v1"
	FullCRDName = CRDPlural + "." + CRDGroup
)

// Create the CRD resource, ignore error if it already exists
func CreateCRD(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: FullCRDName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   CRDGroup,
			Version: CRDVersion,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names:   apiextv1beta1.CustomResourceDefinitionNames{
				Plural: CRDPlural,
				Kind:   reflect.TypeOf(SparkCluster{}).Name(),
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err

	// Note the original apiextensions example adds logic to wait for creation and exception handling
}



// THIS IS CRD FOR SPARK CLUSTER:


type SparkCluster struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec		SparkClusterSpec `json:"spec"`
	Status      SparkClusterStatus `json:"status,omitempty"`
}
type SparkClusterStatus struct{
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

// TODO: Following same format as ClusterConfig.go in oshinko-cli
type SparkClusterSpec struct{
	SparkMasterName string `json"sparkmastername"`
	SparkWorkerName string `json"sparkworkername"`
	Image string `json"image"`
	Workers int32 `json:"workers"`
	SparkMetrics string `json:"sparkmetrics"`
	Notebook string `json:"notebook"`
	Middleware string `json:"middleware"`
	Alertrules string `json:"alertrules"`
}



type SparkClusterList struct {
	meta_v1.TypeMeta `json:",inline"`
	meta_v1.ListMeta `json:"metadata"`
	Items            []SparkCluster `json:"items"`
}

// Create a  Rest client with the new CRD Schema
var SchemeGroupVersion = schema.GroupVersion{Group: CRDGroup, Version: CRDVersion}
var JobSchemeGroupVersion = schema.GroupVersion{Group: JobCRDGroup, Version: JobCRDVersion}


func JobaddKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(JobSchemeGroupVersion,
		&SparkJob{},
		&SparkJobList{},
	)

	meta_v1.AddToGroupVersion(scheme, JobSchemeGroupVersion)
	return nil
}


func JobNewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	SchemeBuilder := runtime.NewSchemeBuilder(JobaddKnownTypes)
	if err := SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	config := *cfg
	config.GroupVersion = &JobSchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, nil, err
	}
	return client, scheme, nil
}



func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&SparkCluster{},
		&SparkClusterList{},
	)

	meta_v1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func NewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	SchemeBuilder := runtime.NewSchemeBuilder(addKnownTypes)
	if err := SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	config := *cfg
	config.GroupVersion = &SchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, nil, err
	}
	return client, scheme, nil
}


const (
	JobCRDPlural   = "sparkjobs"
	JobCRDGroup    = "radanalytics.redhat.com"
	JobCRDVersion  = "v1"
	JobFullCRDName = JobCRDPlural + "." + JobCRDGroup
)



func CreateJobCRD(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: JobFullCRDName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   JobCRDGroup,
			Version: JobCRDVersion,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names:   apiextv1beta1.CustomResourceDefinitionNames{
				Plural: JobCRDPlural,
				Kind:   reflect.TypeOf(SparkJob{}).Name(),
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err

	// Note the original apiextensions example adds logic to wait for creation and exception handling
}

type SparkJob struct {
	meta_v1.TypeMeta   `json:",inline"`
	meta_v1.ObjectMeta `json:"metadata"`
	Spec		SparkJobSpec `json:"spec"`
	Status      SparkJobStatus `json:"status,omitempty"`
}
type SparkJobStatus struct{
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

type SparkJobSpec struct{
	// Look up spark cluster and then run spark job against this sparkcluster
	SparkMasterURL string `json"sparkclustername"`
	AppName string `json"appname"`
	AppArgs string `json"appargs"`
	AppClass string `json"appclass"`
	SourceCode string `json"sourcecode"`
	Image string `json"image"`
	Ephemeral bool `json"ephemeral"`
}



type SparkJobList struct {
	meta_v1.TypeMeta `json:",inline"`
	meta_v1.ListMeta `json:"metadata"`
	Items            []SparkJob `json:"items"`
}


