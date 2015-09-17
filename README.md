# Azure Plugin for Cloudera Director
The Cloudera Director Azure Plugin is an implementation of the [Cloudera Director Service Provider Interface](https://github.com/cloudera/director-spi) for the [Microsoft Azure](https://azure.microsoft.com) cloud platform.
It presently supports provisioning Azure compute resources.

## Overview

These instructions describe how to configure and exercise the Cloudera Director Azure Plugin.
Prior to using this plugin, you will need to have an [Azure subscription](https://azure.microsoft.com/en-us/pricing/purchase-options/). 

## Prerequisites
Prior to using this plugin, you will also need to have a [service principal](https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal/)
Please click the service principal link to collect the following information. 
You will need them as the environment variables for the plugin live tests. 
They are also use as credential for the director server.

* $SUBSCRIPTION_ID
* $TENANT_ID
* $CLIENT_ID
* $CLIENT_SECRET
* Maven installed.
* [Enable programmatic deployment](https://azure.microsoft.com/en-us/blog/working-with-marketplace-images-on-azure-resource-manager/) for the virtual machine image in the subscription.

## Configure Microsoft Azure to support Cloudera Distribution of Hadoop (CDH) and Cloudera Director

CDH requires forward and reverse hostname resolution, however the default DNS service provided by [Azure only supports forward resolution](https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-name-resolution-for-vms-and-role-instances/).
Therefore, [name resolution using your own DNS server](https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-name-resolution-for-vms-and-role-instances/#name-resolution-using-your-own-dns-server) is required.
This script sets up a very basic forward and reverse DDNS service. It can be use as a starting point. [FIXME add link to Jason's doc and/or director manual]()

The [default resources limit per subscription](https://azure.microsoft.com/en-us/documentation/articles/azure-subscription-service-limits/) is quite low for a typical CDH cluster. They need to be increased, especially core count. 
Please file a support ticket at [Azure portal](https://portal.azure.com/)
The officially supported [instance types](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-linux-sizes/) on Azure are STANDARD_DS14 and STANDARD_DS13, with 16 cores and 8 cores per machine respectively. 

## Configure the Tests
For mock tests there is nothing to configure, they are designed to be run out of the box.

For live tests accessing the backend, Several Azure resources need to be configured before running the test.
At [Azure portal](https://portal.azure.com/), you must create the following resources in the region where the tests will be run.
* ResourceGroup named: pluginUnitTestResourceGroup
* Virtual Network (under the resource group above) named: TestVnet
* Network Security Group (under the resource group above) named: TestNsg
* Availability Set (under the resource group above) named: TestAs

## Run the Tests
To run mock tests without access to the backend
```bash
mvn clean install
```

To run live test accessing the backend
```bash
mvn clean install \
-Dtest.azure.live=true 
-DsubscriptionId=$SUBCRIPTION_ID
-DtenantId=$TENANT_ID
-DclientId=$CLIENT_ID 
-DclientSecret=$CLIENT_SECRET
```

## Install the Plugin in Cloudera Director
There are instructions on plugin installation in the Cloudera Director Service Provider Interface [documentation](https://github.com/cloudera/director-spi#installing-the-plugin).

### Code Dependencies
- See [Azure SDK for Java](https://github.com/Azure/azure-sdk-for-java).
- More [documentation on using the Azure SDK](https://azure.microsoft.com/en-us/documentation/articles/java-download-windows/)

## Copyright and License
Copyright © 2016 Cloudera. Licensed under the Apache License.

See [LICENSE.txt](https://github.com/cloudera/director-azure-plugin/blob/master/LICENSE) for more information.
