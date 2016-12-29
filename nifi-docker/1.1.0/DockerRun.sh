#!/bin/bash
DOCKER_IMAGE="$(cat DockerImage.txt)"
docker run -it -d -p 8080:8080 -p 8181:8181 $DOCKER_IMAGE