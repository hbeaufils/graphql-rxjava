#!/bin/bash

IMAGE_NAME=quicksign/graphql-rxjava-build
CONTAINER_OUTPUT_DIR=/data/build
HOST_OUTPUT_DIR=./outputs

GRADLE_CACHE=graphql-rxjava-gradle-cache

SHORT_GIT_SHA1=${SHORT_GIT_SHA1}

if [ -f /.dockerinit ] || [ -f /.dockerenv ]; then
    echo
    echo "--------------------------"
    echo "| Building inside docker |"
    echo "--------------------------"
    echo
    echo "SHORT_GIT_SHA1=$SHORT_GIT_SHA1"

    set -x
    set -e

    cd /data

    # Link cache folders
    ln -s /cache/graphql-rxjava/gradle/ /root/.gradle
    
    ./gradlew assemble -PskipSignArchives

else
    echo
    echo "---------------------------"
    echo "| Building outside docker |";
    echo "---------------------------"
    echo

    set -x
    set -e

    # Building build image
    docker build --rm -t $IMAGE_NAME .

    # Create Gradle cache if it doesn't exist already
    docker create --name $GRADLE_CACHE -v /cache/graphql-rxjava/gradle busybox || echo "Gradle cache data container for GraphQL RxJava already exists"

    # Create data container
    OUTPUT_ID=`docker create -v $CONTAINER_OUTPUT_DIR $IMAGE_NAME`
    
    SHORT_GIT_SHA1=`git rev-parse --short HEAD`

    # Run the build in a new container, outputs will be saved in data container
    docker run --rm --volumes-from $OUTPUT_ID --volumes-from $GRADLE_CACHE -e "SHORT_GIT_SHA1=$SHORT_GIT_SHA1" $IMAGE_NAME /data/build.sh

    # Extract outputs from data container into host
    docker run --rm --volumes-from $OUTPUT_ID $IMAGE_NAME tar -zcf - -C $CONTAINER_OUTPUT_DIR . | (mkdir -p $HOST_OUTPUT_DIR; cd $HOST_OUTPUT_DIR; tar -zxmf -)

    # Delete data container and its volume (-v)
    docker rm -v $OUTPUT_ID
fi

# To debug
# docker run --volumes-from graphql-rxjava-gradle-cache -it quicksign/graphql-rxjava-build bash
