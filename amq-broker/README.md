This directory contains the helm configuration for installing AMQ Broker 7.13

It installs the following:

 - AMQ Broker operator 7.13 
 - AMQ Broker (with MQTT Acceptor) 3 broker clustered setup with persistence enabled
 - JAAS config for mtls & user/pwd auth
 - Load Balancer Service and Service Mon for metric scraping
 - RBAC secret for upto 1000 trucks across 3 example sites ( perth, pilbara, tomprice) and consumers for kafka and telegraf(deprecated)