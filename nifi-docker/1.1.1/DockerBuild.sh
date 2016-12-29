#!/bin/bash
DOCKER_IMAGE="$(cat DockerImage.txt)"
docker build -t $DOCKER_IMAGE .