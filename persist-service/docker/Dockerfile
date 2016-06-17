FROM centos:7
MAINTAINER "Robin Verlangen"
RUN yum install -y git wget
RUN wget https://storage.googleapis.com/golang/go1.6.2.linux-amd64.tar.gz && tar -C /usr/local -zxvf go*.tar.gz && cp /usr/local/go/bin/go /usr/bin/go
WORKDIR /usr/local
RUN git clone https://github.com/RobinUS2/presto-bloomfilter.git
RUN ./presto-bloomfilter/persist-service/build.sh
CMD ["/usr/local/presto-bloomfilter/persist-service/persist-service"]
