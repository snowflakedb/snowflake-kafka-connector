#!/bin/bash

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

# assume that we are on EC2 Ubuntu 18.04
echo -e "=== This script is only tested on Ubuntu 18.04 and Mac OSX 10.15.3 ==="
if [[ "$OSTYPE" == "linux-gnu" ]]; then
    # Ubuntu 18.04
    echo -e "=== Running on Linux ==="

    # install docker
    sudo apt update
    sudo apt -y upgrade
    sudo apt -y install wget apt-transport-https ca-certificates curl software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
    sudo apt update
    apt-cache policy docker-ce
    sudo apt -y install docker-ce
    sudo usermod -aG docker ${USER}
    docker

    # install kubectl
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
    echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list
    sudo apt update
    sudo apt -y install kubectl

    # install minikube
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube_1.8.1-0_amd64.deb &&
        sudo dpkg -i minikube_1.8.1-0_amd64.deb
    rm minikube_1.8.1-0_amd64.deb

    # install helm
    curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash

    # install jq, jkd, mvn, pip3
    sudo apt -y install jq openjdk-8-jdk maven python3-pip

    # install confluent producer and snowflake python driver
    pip3 install --user "confluent-kafka[avro]"
    pip3 install --user --upgrade snowflake-connector-python==2.7.4

elif [[ "$OSTYPE" == "darwin"* ]]; then
    # Mac OSX
    echo -e "=== Running on Mac OSX ==="
    command -v docker >/dev/null 2>&1 || error_exit "Please install docker by yourself."

    # install homebrew
    command -v brew >/dev/null 2>&1 ||
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    brew update

    brew tap adoptopenjdk/openjdk
    brew cask install adoptopenjdk8

    brew install kubectl minikube helm jq maven

else

    # Unknown.
    echo -e "=== Running on unknown OS, try at your own risk ==="
fi

command -v minikube >/dev/null 2>&1 || error_exit "Require minikube but it's not installed.  Aborting."
command -v docker >/dev/null 2>&1 || error_exit "Require docker but it's not installed.  Aborting."
command -v helm >/dev/null 2>&1 || error_exit "Require helm but it's not installed.  Aborting."
command -v kubectl >/dev/null 2>&1 || error_exit "Require kubectl but it's not installed.  Aborting."
command -v jq >/dev/null 2>&1 || error_exit "Require jq but it's not installed.  Aborting."
command -v mvn >/dev/null 2>&1 || error_exit "Require mvn but it's not installed.  Aborting."
command -v python3 >/dev/null 2>&1 || error_exit "Require python3 but it's not installed.  Aborting."

# assuming on dev mac, might need to change the values if on AWS
minikube config set memory 8192
minikube config set cpus 4
minikube config set disk-size 20000MB

minikube stop || true
minikube delete || true
minikube start
