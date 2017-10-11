# Azure Plugin for Cloudera Director
The Cloudera Director Azure Plugin is an implementation of the [Cloudera Director Service Provider Interface](https://github.com/cloudera/director-spi) for the [Microsoft Azure](https://azure.microsoft.com) cloud platform.

## Copyright and License
Copyright © 2016-2017 Cloudera. Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Cloudera, the Cloudera logo, and any other product or service names or slogans contained in this document are trademarks of Cloudera and its suppliers or licensors, and may not be copied, imitated or used, in whole or in part, without the prior written permission of Cloudera or the applicable trademark holder.

Hadoop and the Hadoop elephant logo are trademarks of the Apache Software Foundation. Amazon Web Services, the "Powered by Amazon Web Services" logo, Amazon Elastic Compute Cloud, EC2, Amazon Relational Database Service, and RDS are trademarks of Amazon.com, Inc. or its affiliates in the United States and/or other countries. All other trademarks, registered trademarks, product names and company names or logos mentioned in this document are the property of their respective owners. Reference to any products, services, processes or other information, by trade name, trademark, manufacturer, supplier or otherwise does not constitute or imply endorsement, sponsorship or recommendation thereof by us.

Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Cloudera.

Cloudera may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this document. Except as expressly provided in any written license agreement from Cloudera, the furnishing of this document does not give you any license to these patents, trademarks, copyrights, or other intellectual property. For information about patents covering Cloudera products, see http://tiny.cloudera.com/patents.


The information in this document is subject to change without notice. Cloudera shall not be liable for any damages resulting from technical errors or omissions which may be present in this document, or from use of this document.


# Testing

These instructions describe how to run unit and live tests for the Cloudera Director Azure Plugin.


## Prerequisites


### Required environmental information

Before using this plugin you will need:
* An [Azure subscription](https://azure.microsoft.com/en-us/pricing/purchase-options/).
* A [Service Principal](https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal/).
    Use the Service Principal to get the required fields used for testing and authentication:
    * subscription id
    * tenant id
    * client id
    * client secret
* Maven installed.
* [Programmatic deployment enabled](https://azure.microsoft.com/en-us/blog/working-with-marketplace-images-on-azure-resource-manager/) for each virtual machine image used in the subscription.


### Resource limits

Per the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (PDF), the supported instance types used in deploying a CDH cluster are large. Even with small test clusters you'll hit the [default resource limits per subscription](https://azure.microsoft.com/en-us/documentation/articles/azure-subscription-service-limits/).

Increase the limits, especially core count, by filing a support ticket on the [Azure portal](https://portal.azure.com/).


## Building everything

To build everything, from Launchpad's base directory run:
```bash
mvn clean install -DskipTests
```


## Unit tests

Unit tests are simple. From Launchpad's base directory run:
```bash
mvn -e \
-pl plugins/azure/tests \
clean install
```


## Live tests

Live tests are more complicated and require the following:
* The [Service Principal](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal) fields:
    * `$SUBCRIPTION_ID`
    * `$TENANT_ID`
    * `$CLIENT_ID`
    * `$CLIENT_SECRET`
* Paths to public and private ssh keys(`$SSH_PUBLIC_KEY_PATH` and `$SSH_PRIVATE_KEY_PATH`). For keys included with Launchpad use:
    * `-Dtest.azure.sshPublicKeyPath=../../../keys/azure/azure-user`
    * `-Dtest.azure.sshPrivateKeyPath=../../../keys/azure/azure-user.pem`
* A Resource Group name (`$RESOURCE_GROUP_NAME`)
    * If the Resource Group does not exist the tests will create it, along with the supporting resources, and delete it when the tests finish.
    * If the Resources Group does exist the tests will use it and won't delete it. The following resources must be in the existing Resource Group:
        * a Virtual Network named `vn`
        * a Network Security Group named `nsg`
        * a managed Availaiblity Set named `managedAS`
        * an unmanaged Availability Set named `unmanagedAS`
* A Region to use (`$REGION`); this is optional and by default the tests will use `eastus`.
* A specific set of tests to run (`$TEST`) in the format `<Class>` or `<Class>#<Method>` (e.g. `AzureComputeProviderLiveTest#fullCycle`); this is optional and by default all the tests will be run.

To run all Live Tests, from Launchpad's base directory run:
```bash
mvn -e \
-pl plugins/azure/tests \
-Dtest.azure.live \
-DsubscriptionId=${SUBSCRIPTION_ID} \
-DtenantId=${TENANT_ID} \
-DclientId=${CLIENT_ID} \
-DclientSecret=${CLIENT_SECRET} \
-Dtest.azure.sshPublicKeyPath=${SSH_PUBLIC_KEY_PATH} \
-Dtest.azure.sshPrivateKeyPath=${SSH_PRIVATE_KEY_PATH} \
-Dazure.live.region=${REGION} \
-Dazure.live.rg=${RESOURCE_GROUP_NAME} \
-am clean install
```

To run a specific test, from Launchpad's base directory run (note the change from `-am clean install` to `test`):
```bash
mvn -e \
-pl plugins/azure/tests \
-Dtest.azure.live \
-DsubscriptionId=${SUBSCRIPTION_ID} \
-DtenantId=${TENANT_ID} \
-DclientId=${CLIENT_ID} \
-DclientSecret=${CLIENT_SECRET} \
-Dtest.azure.sshPublicKeyPath=${SSH_PUBLIC_KEY_PATH} \
-Dtest.azure.sshPrivateKeyPath=${SSH_PRIVATE_KEY_PATH} \
-Dazure.live.region=${REGION} \
-Dazure.live.rg=${RESOURCE_GROUP_NAME} \
-Dtest=${TEST}
test
```


# Deploying

These instructions describe how to deploy the Cloudera Director Azure Plugin.


## Prerequisites


### Required fields

Before using this plugin you will need:
* An [Azure subscription](https://azure.microsoft.com/en-us/pricing/purchase-options/).
* A [Service Principal](https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal/).
    Use the Service Principal to get the required fields used for testing and authentication:
    * subscription id
    * tenant id
    * client id
    * client secret
* Maven installed.
* [Programmatic deployment enabled](https://azure.microsoft.com/en-us/blog/working-with-marketplace-images-on-azure-resource-manager/) for each virtual machine image used in the subscription.


## Pre deployment setup


### Forward and reverse DNS

Cloudera Distribution of Hadoop (CDH) and Cloudera Director require forward and reverse hostname resolution; this is not currently supported by Azure ([Azure only supports forward resolution](https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-name-resolution-for-vms-and-role-instances/)) and it's required to do [name resolution using your own DNS server](https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-name-resolution-for-vms-and-role-instances/#name-resolution-using-your-own-dns-server).

[More instructions](https://www.cloudera.com/documentation/director/latest/topics/director_get_started_azure_ddns.html).


### Resource limits

Per the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (PDF), the supported instance types used in deploying a CDH cluster are large. Even with small test clusters you'll hit the [default resource limits per subscription](https://azure.microsoft.com/en-us/documentation/articles/azure-subscription-service-limits/).

Increase the limits, especially core count, by filing a support ticket on the [Azure portal](https://portal.azure.com/).


# Understanding and Changing the Cloudera Director Azure Plugin Config Files

**IMPORTANT:** The [Cloudera Enterprise Reference Architecture for Azure Deployments](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (PDF) is the authoritative document for supported deployment configurations in Azure.

There are two files that the Cloudera Director Azure Plugin uses to change settings:

* `images.conf`
* `azure-plugin.conf`

The files and their uses are explained below.

## `images.conf`

**What does `images.conf` do?**

The `images.conf` file defines the VM images Cloudera Director can use to provision VMs. The `images.conf` file in this repository is continuously updated with the latest supported VM images. The latest supported images can be found in the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (PDF).


**How do I add another image I want to use?**

1. Take the `images.conf` file found in this repository and add a new image using the following format:
    ```
    image-name { # this is used to reference this image within Cloudera Director
      # these fields uniquely identify the image, their values are found within Azure
      publisher: publisher_name
      offer: offer_name
      sku: sku_name
      version: version_name
    }
    ```
2. On Cloudera Director server, copy your modified `images.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/images.conf`.
3. Restart Cloudera Director with `sudo service cloudera-director-server restart`.
4. Now you can use your newly defined image when deploying clusters. Note that in the Cloudera Director UI you won't see the image-name in the dropdown list - just type it in manually and it will work.


**Any caveats I should be aware of?**

* When you copy your modified `images.conf` file to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/images.conf` make sure you're putting it in the latest `azure-provider-[version]` directory.

* After updating your version of Cloudera Director you'll need to copy your `.conf` files to the latest `azure-provider-[version]` directory.


## `azure-plugin.conf`

**What does `azure-plugin.conf` do?**

The `azure-plugin.conf` file defines settings that Cloudera Director uses to validate VMs before provisioning. There are a bunch of fields, here are the important ones:

* `provider` > `supported-regions`: this is the list of regions that a cluster can be deployed into. Only regions that support premium storage should be added to the list - that list can be found [here](https://azure.microsoft.com/en-us/regions/services/).
* `instance` > `supported-instances`: this is the list of supported instance sizes that can be used. Only certain sizes have been certified.
* `instance` > `supported-premium-data-disk-sizes`: this is the list of supported disk sizes that can be used. Only certain sizes have been certified.
* `instance` > `maximum-standard-data-disk-size`: this is the maximum size standard storage disk that can be used. Only certain sizes have been certified.

The latest supported instances, premium disks, and standard disks can be found in the [Azure Reference Architecture](http://www.cloudera.com/documentation/other/reference-architecture/PDF/cloudera_ref_arch_azure.pdf) (PDF).

**How do I add a new region to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new region to the `provider` > `supported-regions` list. The plugin will replace its internal list with this list so make sure you keep all of the supported regions that are already defined in `azure-plugin.conf`
2. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
3. Restart Cloudera Director with `sudo service cloudera-director-server restart`
4. Now you can use your newly defined region when deploying clusters.


**How do I add a new instance to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new instance to the `instance`>`supported-instances` list. The plugin will replace its internal list with this list so make sure you keep all of the supported regions that are already defined in `azure-plugin.conf`
2. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
3. Restart Cloudera Director with `sudo service cloudera-director-server restart`
4. Now you can use your newly defined instance when deploying clusters.


**How do I add a new Premium disk size to use?**

1. Take the `azure-plugin.conf` file found in this repository and **add** a new disk size to the `instance`>`supported-premium-data-disk-sizes` list. The plugin will replace its internal list with this list so make sure you keep all of the supported disk sizes that are already defined in `azure-plugin.conf`
2. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
3. Restart Cloudera Director with `sudo service cloudera-director-server restart`
4. Now you can use your newly defined Premium disk when deploying clusters.


**How do I change the maximum Standard disk size to use?**

1. Take the `azure-plugin.conf` file found in this repository and **change** the `instance`>`maximum-standard-data-disk-size` value.
2. On Cloudera Director server, copy your modified `azure-plugin.conf` to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf`.
3. Restart Cloudera Director with `sudo service cloudera-director-server restart`
4. Now you can use your newly defined Standard disk when deploying clusters.


**Any caveats I should be aware of?**

When you use a custom `azure-plugin.conf` any keys defined replace those defined internally, so you must append to lists rather than creating a list with only the new values. For example, if you were to add an instance to `instance` > `supported-instances` you would need to keep all of the currently defined supported instances and append yours:

    ```
    supported-instances: [
        "STANDARD_DS15_V2",
        "STANDARD_DS14",
        "STANDARD_DS14_V2"
        "STANDARD_DS13",
        "STANDARD_DS13_V2",
        "STANDARD_DS12_V2",
        "STANDARD_GS5",
        "STANDARD_GS4",
        "STANDARD_D15_V2",
        "STANDARD_D14_V2"
        "STANDARD_D13_V2",
        "STANDARD_D12_V2",
        "YOUR_NEW_INSTANCE"
    ]
    ```
    If you were to have a list with only `YOUR_NEW_INSTANCE` the rest of the instances would stop working.

When you copy your modified `azure-plugin.conf` file to `/var/lib/cloudera-director-plugins/azure-provider-*/etc/azure-plugin.conf` make sure you're putting it in the latest `azure-provider-[version]` directory.

After updating your version of Cloudera Director you'll need to copy your `.conf` files to the latest `azure-provider-[version]` directory.


**What about the other fields?**

* `provider` > `azure-backend-operation-polling-timeout-second` defines the timeout in seconds for a task interacting with the Azure backend. Chances are you won't need to change this.
* `instance` > `instance-prefix-regex` defines the regex that instances names get validated against. This will only change if Azure changes their backend. Don't change this.
* `instance` > `dns-fqdn-suffix-regex` defines the regex that the FQDN Suffix gets validated against. This will only change if Azure changes their backend. Don't change this.
* `instance` > `azure-disallowed-usernames` defines the list of usernames disallowed by Azure. This will only change if Azure changes their backend. Don't change this.
* `instance` > `azure-validate-resources` determines wether or not to run provider and instance template validator checks. You shouldn't need to change this.
* `instance` > `azure-validate-credentials` determines wether or not to run Azure credential checks. You shouldn't need to change this.
