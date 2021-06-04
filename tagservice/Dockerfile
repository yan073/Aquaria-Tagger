# COPY FROM https://github.com/larsjuhljensen/tagger
# VERSION       4
# image name: yan047/trialtagger:4

FROM centos:7.2.1511
LABEL maintainer="bo.yan@csiro.au"

# install base dependencies
RUN yum -y install git swig gcc gcc-c++ make python-devel boost boost-devel unzip wget epel-release
RUN yum -y install python-pymongo python-pip
RUN pip install Flask

WORKDIR /app

# clone and build tagger
RUN git clone https://github.com/larsjuhljensen/tagger.git \
    && cd tagger \
    && make

ENV FLASK_APP webapp.py
ENV CONFIG_FILE app.cfg

ADD app.cfg /app/tagger
ADD src /app/tagger

VOLUME /data
WORKDIR /app/tagger

EXPOSE 5000