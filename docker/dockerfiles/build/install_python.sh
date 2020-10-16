#!/bin/bash

set -e

#PYTHON_VERSION="3.7.4"
#PYTHON_MINOR_VERSION="3.7"

APP_HOME="/opt/beer-garden"
mkdir -p $APP_HOME

LANG="en_US.UTF-8"
localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

# Need these things
yum install -y \
	wget \
	gcc \
	gcc-c++ \
	zlib \
	zlib-devel \
	curl-devel \
	libffi-devel \
	openssl-devel \
	readline-devel \
	rpm-build \
	tar \
	make \
	bzip2-devel \
	sqlite-devel


# Build python
mkdir -p /usr/src/
cd /usr/src/

PYTHON_VERSIONS=("3.6.5" "3.7.4" "3.8.6")
PYTHON_DEFAULT="3.7"

for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"
do
	echo "Downloading and extracting Python-$PYTHON_VERSION"
	wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz
	tar -xf Python-${PYTHON_VERSION}.tar.xz
	rm -f Python-${PYTHON_VERSION}.tar.xz

	echo "Building and installing Python-$PYTHON_VERSION"
	wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz
	cd Python-$PYTHON_VERSION
	./configure --prefix=$APP_HOME --enable-optimizations
	make altinstall prefix=$APP_HOME exec-prefix=$APP_HOME
	#$APP_HOME/bin/python${PYTHON_MINOR_VERSION} -m "ensurepip"
done

rm -rf /usr/src/ /opt/beer-garden/share
find $APP_HOME -type d '(' -name '__pycache__' -o -name 'test' -o -name 'tests' ')' -exec rm -rfv '{}' +
find $APP_HOME -type f '(' -name '*.py[co]' -o -name '*.exe' ')' -exec rm -fv '{}' +

cd $APP_HOME/bin
ln -fs python${PYTHON_DEFAULT} python
ln -fs pip${PYTHON_DEFAULT} pip


# Build ruby
#mkdir -p /usr/src/ruby
#
#cd /usr/src
#curl https://cache.ruby-lang.org/pub/ruby/2.7/ruby-2.7.2.tar.gz -o ruby-2.7.2.tar.gz
#tar -xC /usr/src/ruby ruby-2.7.2.tar.gz
#rm ruby-2.7.2.tar.gz
#
#cd /usr/src/ruby
#./configure --disable-install-doc
#make
#make install
#
#cd /usr/src
#rm -rf /usr/src/ruby
#
#gem install --no-ri --no-rdoc fpm
