FROM golang
ADD _output/spark-crd /spark-crd
USER 185
ENTRYPOINT ["/spark-crd"]