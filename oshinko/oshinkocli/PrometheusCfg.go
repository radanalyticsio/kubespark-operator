package oshinkocli

import "fmt"

func GetInitPromConfig() string {
	return `
    global:
      scrape_interval:     5s
      evaluation_interval: 5s
    scrape_configs:
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
