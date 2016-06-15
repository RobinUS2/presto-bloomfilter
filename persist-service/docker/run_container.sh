#!/bin/bash
docker run --name prestobloomfilterpersist \
	-v /etc/prestobloomfilterpersist.json:/etc/prestobloomfilterpersist.json \
	-d \
	--restart=always \
	--ulimit nofile=262144:262144 \
	--memory="1G" \
	--net=host \
	prestobloomfilterpersist
