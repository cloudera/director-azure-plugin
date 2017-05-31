/*
 * Copyright (c) 2016 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.director.azure.compute.instance;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Property;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.microsoft.azure.management.storage.models.AccountType;

/**
 * Azure instance template configuration properties.
 */
public enum AzureComputeInstanceTemplateConfigurationProperty implements ConfigurationPropertyToken {

  IMAGE(new SimpleConfigurationPropertyBuilder()
    .configKey(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE.unwrap()
      .getConfigKey())
    .name("Image Alias")
    .addValidValues(
      "cloudera-centos-6-latest",
      "cloudera-centos-68-latest",
      "cloudera-centos-72-latest",
      "redhat-rhel-67-latest",
      "redhat-rhel-68-latest",
      "redhat-rhel-72-latest")
    .defaultDescription("The image alias from plugin configuration.")
    .defaultErrorMessage("Image alias is mandatory")
    .widget(ConfigurationProperty.Widget.OPENLIST)
    .required(true)
    .build()),

  VMSIZE(new SimpleConfigurationPropertyBuilder()
    .configKey(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.TYPE.unwrap()
      .getConfigKey())
    .name("VirtualMachine Size")
    .addValidValues(
      "STANDARD_DS15_V2",
      "STANDARD_DS14",
      "STANDARD_DS14_V2",
      "STANDARD_DS13",
      "STANDARD_DS13_V2",
      "STANDARD_DS12_V2",
      "STANDARD_GS5",
      "STANDARD_GS4",
      "STANDARD_D15_V2",
      "STANDARD_D14_V2",
      "STANDARD_D13_V2",
      "STANDARD_D12_V2")
    .defaultDescription(
      "The machine type.<br />" +
        "<a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-linux-sizes/'>More Information</a>")
    .defaultErrorMessage("VirtualMachine Size")
    .widget(ConfigurationProperty.Widget.OPENLIST)
    .required(true)
    .build()),

  COMPUTE_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
    .configKey("computeResourceGroup")
    .name("Compute Resource Group")
    .defaultDescription(
      "The Resource Group where the compute resources such as VM instances and availability sets " +
        "will be created.<br />" +
        "<a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/resource-group-overview/#resource-groups'>More Information</a>")
    .defaultErrorMessage("Compute resource group is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  VIRTUAL_NETWORK_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
    .configKey("virtualNetworkResourceGroup")
    .name("Virtual Network Resource Group")
    .defaultDescription("The Resource Group where the Virtual Network is located.")
    .defaultErrorMessage("Resource Group is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  VIRTUAL_NETWORK(new SimpleConfigurationPropertyBuilder()
    .configKey("virtualNetwork")
    .name("Virtual Network")
    .defaultDescription(
      "The Virtual Network for the deployment. This must exist in the Virtual Network Resource " +
        "Group you selected.<br />" +
        "<a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-overview/'>More " +
        "Information</a>")
    .defaultErrorMessage("Virtual network is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  SUBNET_NAME(new SimpleConfigurationPropertyBuilder()
    .configKey("subnetName")
    .name("Subnet Name")
    .defaultValue("default")
    .defaultDescription(
      "The name of the subnet to use for VMs. This subnet resource must be under the virtual " +
        "network resource you specified.")
    .defaultErrorMessage("Subnet Name is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  HOST_FQDN_SUFFIX(new SimpleConfigurationPropertyBuilder()
    .configKey("hostFqdnSuffix")
    .name("Host FQDN suffix")
    .defaultDescription(
      "The private domain name used to create a FQDN for each host. </ br>" +
        "Note: Azure provided DNS service does not support reverse DNS lookup for private IP " +
        "addresses. You must setup a dedicated DNS service with reverse lookup support and use " +
        "this FQDN Suffix.")
    .defaultErrorMessage("Host FQDN suffix is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  NETWORK_SECURITY_GROUP_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
    .configKey("networkSecurityGroupResourceGroup")
    .name("Network Security Group Resource Group")
    .defaultDescription("The Resource Group where the Network Security Group is located.")
    .defaultErrorMessage("Resource Group is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  NETWORK_SECURITY_GROUP(new SimpleConfigurationPropertyBuilder()
    .configKey("networkSecurityGroup")
    .name("Network Security Group")
    .defaultDescription(
      "The Network Security Group for the deployment. This must exist in the Network Security " +
        "Group Resource Group you selected.<br />" +
        "<a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-nsg/'>More " +
        "Information</a>")
    .defaultErrorMessage("Network security group is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  PUBLIC_IP(new SimpleConfigurationPropertyBuilder()
    .configKey("publicIP")
    .name("Public IP")
    .addValidValues(
      "Yes",
      "No")
    .defaultValue("No")
    .defaultDescription("Whether to attach public IP for the VM.")
    .widget(ConfigurationProperty.Widget.OPENLIST)
    .required(true)
    .build()),

  AVAILABILITY_SET(new SimpleConfigurationPropertyBuilder()
    .configKey("availabilitySet")
    .name("Availability Set")
    .defaultDescription(
      "The Availability Set for the deployment. This must exist in the Compute Resource Group " +
        "you specified earlier.<br />" +
        "<a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/" +
        "virtual-machines-windows-manage-availability/'>More " +
        "Information</a>")
    .widget(ConfigurationProperty.Widget.TEXT)
    .required(true)
    .build()),

  STORAGE_ACCOUNT_TYPE(new SimpleConfigurationPropertyBuilder()
    .configKey("storageAccountType")
    .name("Storage Account Type")
    .addValidValues(
      AccountType.PremiumLRS.toString(),
      AccountType.StandardLRS.toString())
    .defaultDescription("The Storage Account Type to use.")
    .defaultValue(Configurations.AZURE_DEFAULT_STORAGE_ACCOUNT_TYPE)
    .widget(ConfigurationProperty.Widget.OPENLIST)
    .required(false)
    .build()),

  DATA_DISK_COUNT(new SimpleConfigurationPropertyBuilder()
    .configKey("dataDiskCount")
    .name("Data Disk Count")
    .defaultDescription("The number of data disks to create.")
    .defaultValue("5")
    .type(Property.Type.INTEGER)
    .widget(ConfigurationProperty.Widget.NUMBER)
    .build()),

  DATA_DISK_SIZE(new SimpleConfigurationPropertyBuilder()
    .configKey("dataDiskSize")
    .name("Data Disk Size in GiB")
    .addValidValues(
      "1023",
      "512")
    .defaultDescription("The size of the data disks in GiB.<br />P30: 1023GiB<br />P20: 512GiB" +
      "<br /><a target='_blank' href='https://azure.microsoft.com/en-us/documentation/articles/" +
      "storage-premium-storage/'>More Information</a>")
    .defaultValue("1023")
    .type(Property.Type.INTEGER)
    .widget(ConfigurationProperty.Widget.OPENLIST)
    .required(false)
    .build());

  /**
   * The configuration property.
   */

  private final ConfigurationProperty configurationProperty;

  /**
   * Creates a configuration property token with the specified parameters.
   *
   * @param configurationProperty the configuration property
   */
  AzureComputeInstanceTemplateConfigurationProperty(ConfigurationProperty configurationProperty) {
    this.configurationProperty = configurationProperty;
  }

  @Override
  public ConfigurationProperty unwrap() {
    return configurationProperty;
  }
}
