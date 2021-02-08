#!/bin/bash
base_path=$(dirname $0)
cd $base_path
base_path=$(pwd)
cd $base_path

old_version=$(cat $base_path/version)
git pull
version=$(git log | grep -e 'commit [a-zA-Z0-9]*'|wc -l)
echo old version is $old_version,new version is $version!

if [[ $old_version == $version ]];then
    echo It does not to build!
    exit
fi
source /etc/profile

export GOPATH=$GOPATH:/Users/richer/go/
export CGO_ENABLED=0

cd $base_path/server
go build -tags postgres

if [[ $? != 0 ]];then
    echo ERROR: go build!
    exit 1
fi

cd $base_path/tinode-db
go build -tags postgres

if [[ $? != 0 ]];then
    echo ERROR: go build!
    exit 2
fi

echo $version > $base_path/version
