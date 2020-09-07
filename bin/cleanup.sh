#!/bin/sh

docker stop oreilly postgres adminer
docker container rm oreilly postgres adminer
docker rmi oreilly:latest postgres:9.6 adminer:latest python:3.8-slim
docker network rm oreilly_oreilly