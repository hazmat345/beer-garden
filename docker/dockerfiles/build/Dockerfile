# Centos 7 Build Image
#
# This image is for building a CentOS-7/RHEL-7 compatible RPM for beer-garden
#
# It comes with:
#
#  * Python 3.7.4 installed
#  * FPM installed for package management
#  * Build essential stuff
#
# Tag: centos7-python3.7

FROM centos:7 AS base

ENV LANG=en_US.UTF-8
#ENV LANG=en_US.UTF-8 \
#    PYTHON_VERSION=3.7.4 \
#    PYTHON_MINOR_VERSION=3.7 \
#    APP_HOME=/opt/beer-garden

COPY install_python.sh /install_python.sh
RUN /install_python.sh


FROM ruby:2.7 AS ruby

COPY --from base /opt/beer-garden /opt/beer-garden
