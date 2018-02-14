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
package client

import (
	"github.com/zmhassan/sparkcluster-crd/crd"

	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/apimachinery/pkg/runtime"
)

// This file implement all the (CRUD) client methods we need to access our CRD object

func SparkJobCrdClient(cl *rest.RESTClient, scheme *runtime.Scheme, namespace string) *sparkJobcrdclient {
	return &sparkJobcrdclient{cl: cl, ns: namespace, plural: crd.JobCRDPlural,
		codec: runtime.NewParameterCodec(scheme)}
}

type sparkJobcrdclient struct {
	cl     *rest.RESTClient
	ns     string
	plural string
	codec  runtime.ParameterCodec
}

func (f *sparkJobcrdclient) Create(obj *crd.SparkJob) (*crd.SparkJob, error) {
	var result crd.SparkJob
	err := f.cl.Post().
		Namespace(f.ns).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *sparkJobcrdclient) Update(obj *crd.SparkJob) (*crd.SparkJob, error) {
	var result crd.SparkJob
	err := f.cl.Put().
		Namespace(f.ns).Resource(f.plural).
		Body(obj).Do().Into(&result)
	return &result, err
}

func (f *sparkJobcrdclient) Delete(name string, options *meta_v1.DeleteOptions) error {
	return f.cl.Delete().
		Namespace(f.ns).Resource(f.plural).
		Name(name).Body(options).Do().
		Error()
}

func (f *sparkJobcrdclient) Get(name string) (*crd.SparkJob, error) {
	var result crd.SparkJob
	err := f.cl.Get().
		Namespace(f.ns).Resource(f.plural).
		Name(name).Do().Into(&result)
	return &result, err
}

func (f *sparkJobcrdclient) List(opts meta_v1.ListOptions) (*crd.SparkJobList, error) {
	var result crd.SparkJobList
	err := f.cl.Get().
		Namespace(f.ns).Resource(f.plural).
		VersionedParams(&opts, f.codec).
		Do().Into(&result)
	return &result, err
}

// Create a new List watch for our TPR
func (f *sparkJobcrdclient) NewListWatch() *cache.ListWatch {
	return cache.NewListWatchFromClient(f.cl, f.plural, f.ns, fields.Everything())
}
