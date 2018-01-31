// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"strings"
 	"github.com/spf13/cobra"
	"github.com/zmhassan/sparkcluster-crd/oshinko/config"
	"flag"
	"os"
	"github.com/zmhassan/sparkcluster-crd/oshinko/oshinkocli"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "gets details about a spark cluster",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		clicfg:=SparkCliConfig{}

		for _, x := range args  {
 			cliArgs:=strings.Split(x, "=")
			if cliArgs[0] == "name" {
				clicfg.clustername = cliArgs[1]
			}else {
				fmt.Println("Unknown value")
			}
		}
		GetSparkCluster(clicfg)

	},
}

func GetSparkCluster(cliconfig SparkCliConfig) {
	// TODO: Get deployment for clusterName + '-spark-master'
	kubeconf := flag.String("kubeconf", os.Getenv("HOME")+"/.kube/config", "Path to a kube config. Only required if out-of-cluster.")

	flag.Parse()

	config, err := oshinkoconfig.GetKubeCfg(*kubeconf)

	if err != nil {
		panic(err.Error())
	}

	dep:=oshinkocli.FindCluster(config, cliconfig.clustername)

	fmt.Println("Name	 Namespace	 SparkSubmitURL")
	fmt.Println(cliconfig.clustername,"	",dep.Namespace,"	",dep.Name+":7077")

}

func init() {
	rootCmd.AddCommand(getCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
