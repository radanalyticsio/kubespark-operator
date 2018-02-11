package oshinkocli

import "fmt"

//TODO: Customize the alertmanager url
func GetInitPromConfig() string {
	return `
    global:
      scrape_interval:     5s
      evaluation_interval: 5s
    alerting:
      alertmanagers:
      - static_configs:
        - targets:
          - alertmanager-erik-spark-master-service:9093

    rule_files:
      - "simple_rule.yml"

    scrape_configs:

  `
}

func GetSimpleRule() string{
	return `
    groups:
    - name: spark.rules
      rules:
      - alert: SparkOutage
        expr: up == 0
        for: 5s
        labels:
          severity: critical
        annotations:
          description: erik spark cluster is down and out
          summary: erik spark Instance down

`
}

func AddSparkNodeToMonitor(hostname string, target string) string {
	return fmt.Sprintf(
		`
      - job_name: '%s'
        static_configs:
          - targets: ['%s']
`, hostname, target)

}
