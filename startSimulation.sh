#!/bin/bash

export AMBARI_HOST=$(hostname -f)

export CLUSTER_NAME=$(curl -u admin:admin -X GET http://$AMBARI_HOST:8080/api/v1/clusters |grep cluster_name|grep -Po ': "(.+)'|grep -Po '[a-zA-Z0-9!$\-]+')

if [[ -z $CLUSTER_NAME ]]; then
        echo "Could not connect to Ambari Server. Please run this script on the same host where Ambari Server is installed."
        exit 1
else
       	echo "*********************************CLUSTER NAME IS: $CLUSTER_NAME"
fi

getNifiHost () {
       	NIFI_HOST=$(curl -u admin:admin -X GET http://$AMBARI_HOST:8080/api/v1/clusters/$CLUSTER_NAME/services/NIFI/components/NIFI_MASTER|grep "host_name"|grep -Po ': "([a-zA-Z0-9\-_!?.]+)'|grep -Po '([a-zA-Z0-9\-_!?.]+)')

       	echo $NIFI_HOST
}

NIFI_HOST=$(getNifiHost)
NIFI_HOST_IP=$(getent hosts $NIFI_HOST | awk '{ print $1 }')

echo "*********************************NIFI HOST: $NIFI_HOST"
echo "*********************************NIFI HOST IP: $NIFI_HOST_IP"

echo "*********************************Starting RetailStore Simulation..."
nohup java -jar DeviceSimulator-0.0.1-SNAPSHOT-jar-with-dependencies.jar RetailStore 1000 Simulation $NIFI_HOST_IP > RetailStoreSim.log 2>&1&
echo $! > RetailStoreSim.pid

echo "*********************************Starting SocialMedia Simulation..."
nohup java -jar DeviceSimulator-0.0.1-SNAPSHOT-jar-with-dependencies.jar SocialMedia 1000 Simulation $NIFI_HOST_IP > SocialMediaSim.log 2>&1&
echo $! > SocialMediaSim.pid

exit 0