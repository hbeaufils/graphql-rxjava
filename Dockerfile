
FROM ubuntu:12.04
MAINTAINER Hadrien Beaufils <hadrien.beaufils@quicksign.com>

# Never ask for confirmations
ENV DEBIAN_FRONTEND noninteractive
RUN echo "debconf shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections
RUN echo "debconf shared/accepted-oracle-license-v1-1 seen true" | debconf-set-selections

# First, install add-apt-repository and bzip2
RUN apt-get update && apt-get -y install python-software-properties bzip2

# Add oracle-jdk6 to repositories
RUN add-apt-repository ppa:webupd8team/java

# Make sure the package repository is up to date
RUN echo "deb http://archive.ubuntu.com/ubuntu precise universe" >> /etc/apt/sources.list

# Update apt
RUN apt-get update

# Install oracle-jdk6
RUN apt-get -y install oracle-java8-installer

# Fake a fuse install (to prevent ia32-libs-multiarch package from producing errors)
RUN apt-get install -y  libfuse2
RUN cd /tmp ; apt-get download fuse
RUN cd /tmp ; dpkg-deb -x fuse_* .
RUN cd /tmp ; dpkg-deb -e fuse_*
RUN cd /tmp ; rm fuse_*.deb
RUN cd /tmp ; echo -en '#!/bin/bash\nexit 0\n' > DEBIAN/postinst
RUN cd /tmp ; dpkg-deb -b . /fuse.deb
RUN cd /tmp ; dpkg -i /fuse.deb

# Required by NDK
RUN apt-get -y install build-essential

# Install support libraries for 32-bit
#RUN apt-get -y install ia32-libs-multiarch

ENV ANT_VERSION 1.9.6

# Install android sdk and ndk and ant and remove after install (makes tinier images)
RUN wget http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz && \
	tar -xvzf apache-ant-${ANT_VERSION}-bin.tar.gz && \
	mv apache-ant-${ANT_VERSION} /usr/local/apache-ant && \
	rm apache-ant-${ANT_VERSION}-bin.tar.gz

# Add ant to PATH
ENV ANT_HOME /usr/local/apache-ant
ENV PATH $PATH:$ANT_HOME/bin

# Export JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

ADD . /data

# Do the build
CMD /data/build.sh
