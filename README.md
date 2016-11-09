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

## Cloudera Director Azure Plugin Config Files

**IMPORTANT:** The [Cloudera Enterprise Reference Architecture for Azure Deployments](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (RA) is the authoritative document for supported deployment configurations in Azure.

There are two files that the Cloudera Director Azure Plugin uses to change settings:

* `images.conf`
* `azure-plugin.conf`

The files and their uses are explained below.


### `images.conf`

**What does `images.conf` do?**

The `images.conf` file defines the VM images Cloudera Director can use to provision VMs. The `images.conf` file in this repository is continuously updated with the latest supported VM images. The latest supported images can be found in the [RA](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf).


**How do I add another image I want to use?**

1. Take the `images.conf` file found in this repository and add a new image using the following format:
    ```
    image-name { # this is used to reference this image within Cloudera Director
      # these fields uniquely identify the image, their values are found within Azure
      publisher: some_publisher
      offer: offer_name
      sku: sku_name
      version: version_name
    }
    ```
1. On Cloudera Director server, copy your modified `images.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/images.conf`.
1. Restart Cloudera Director with `sudo service cloudera-director-server restart`
1. Now you can use your newly defined image when deploying clusters. Note that in the Cloudera Director UI you won't see the image-name in the dropdown list - just type it in manually and it will work.


**Any caveats I should be aware of?**

* When you copy your modified `images.conf` file to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/images.conf` make sure you're putting it in the latest `azure-provider-[version]` directory.

* After updating your version of Cloudera Director you'll need to copy your `.conf` files to the latest `azure-provider-[version]` directory.


### `azure-plugin.conf`

**What does `azure-plugin.conf` do?**

The `azure-plugin.conf` file defines settings that Cloudera Director uses to validate VMs before provisioning. There are a bunch of fields, here are the important ones:

* `provider` > `supported-regions`: this is the list of regions that a cluster can be deployed into. Only regions that support premium storage should be added to the list - that list can be found [here](https://azure.microsoft.com/en-us/regions/services/).
* `instance` > `supported-instances`: this is the list of supported instance sizes that can be used. Only certain sizes have been certified. The latest supported instances can be found in the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf).
* `instance` > `supported-premium-data-disk-sizes`: this is the list of supported disk sizes that can be used. Only certain sizes have been certified. The latest supported disks can be found in the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf).
* `instance` > `maximum-standard-data-disk-size`: this is the maximum size standard storage disk that can be used. Only certain sizes have been certified. The latest supported standard disk sizes can be found in the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf).

**How do I add a new region to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new region to the `provider` > `supported-regions` list. The plugin will replace its internal list with this list so make sure you keep all of the supported regions that are already defined in `azure-plugin.conf`
1. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
1. Restart Cloudera Director with `sudo service cloudera-director-server restart`
1. Now you can use your newly defined region when deploying clusters.


**How do I add a new instance to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new instance to the `instance`>`supported-instances` list. The plugin will replace its internal list with this list so make sure you keep all of the supported regions that are already defined in `azure-plugin.conf`
1. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
1. Restart Cloudera Director with `sudo service cloudera-director-server restart`
1. Now you can use your newly defined region when deploying clusters.


**How do I add a new disk size to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new disk size to the `instance`>`supported-premium-data-disk-sizes` list. The plugin will replace its internal list with this list so make sure you keep all of the supported disk sizes that are already defined in `azure-plugin.conf`
1. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
1. Restart Cloudera Director with `sudo service cloudera-director-server restart`
1. Now you can use your newly defined region when deploying clusters.


**Any caveats I should be aware of?**

* When you use a custom `azure-plugin.conf` any keys defined replace those defined internally, so you must append to lists rather than creating a list with only the new values. For example, if you were to add an instance to `instance` > `supported-instances` you would need to keep all of the currently defined supported instances and append yours:

    ```
    supported-instances: [
        "STANDARD_DS14",
        "STANDARD_DS14_V2"
        "STANDARD_DS13",
        "STANDARD_DS13_V2",
        "STANDARD_DS12_V2",
        "YOUR_NEW_INSTANCE"
    ]
    ```
    If you were to have a list with only `YOUR_NEW_INSTANCE` the rest of the instances would stop working.

* When you copy your modified `azure-plugin.conf` file to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf` make sure you're putting it in the latest `azure-provider-[version]` directory.

* After updating your version of Cloudera Director you'll need to copy your `.conf` files to the latest `azure-provider-[version]` directory.


**What about the other fields?**

* `provider` > `azure-backend-operation-polling-timeout-second` defines the timeout in seconds for a task interacting with the Azure backend. Chances are you won't need to change this.
* `instance` > `instance-prefix-regex` defines the regex that instances names get validated against. This will only change if Azure changes their backend. Don't change this.
* `instance` > `dns-fqdn-suffix-regex` defines the regex that the FQDN Suffix gets validated against. This will only change if Azure changes their backend. Don't change this.
* `instance` > `azure-disallowed-usernames` defines the list of usernames disallowed by Azure. This will only change if Azure changes their backend. Don't change this.


## Copyright and License
Copyright © 2016 Cloudera. Licensed under the Apache License.

See [LICENSE.txt](https://github.com/cloudera/director-azure-plugin/blob/master/LICENSE) for more information.
