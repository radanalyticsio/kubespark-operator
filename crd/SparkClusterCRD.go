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
	SparkWorkerName string `json"sparkmastername"`
	Image string `json"image"`
	Workers int32 `json:"workers"`
	SparkMetrics string `json:"sparkmetrics"`
}

type SparkClusterList struct {
	meta_v1.TypeMeta `json:",inline"`
	meta_v1.ListMeta `json:"metadata"`
	Items            []SparkCluster `json:"items"`
}

// Create a  Rest client with the new CRD Schema
var SchemeGroupVersion = schema.GroupVersion{Group: CRDGroup, Version: CRDVersion}

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

