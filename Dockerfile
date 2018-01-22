FROM golang
ADD _output/spark-crd /spark-crd
ADD Banner.txt /
USER 185
ENTRYPOINT ["/spark-crd"]