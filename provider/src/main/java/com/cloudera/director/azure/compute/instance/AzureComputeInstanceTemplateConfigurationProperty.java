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
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.Property;
import com.cloudera.director.spi.v2.model.util.SimpleConfigurationPropertyBuilder;
import com.microsoft.azure.management.storage.SkuName;

/**
 * Azure instance template configuration properties.
 */
public enum AzureComputeInstanceTemplateConfigurationProperty implements
    ConfigurationPropertyToken {

  IMAGE(new SimpleConfigurationPropertyBuilder()
      .configKey(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE
          .unwrap().getConfigKey())
      .name("Image URN")
      .addValidValues(
          "cloudera:cloudera-centos-os:6_7:latest",
          "cloudera:cloudera-centos-os:6_8:latest",
          "cloudera:cloudera-centos-os:7_2:latest",
          "cloudera:cloudera-centos-os:7_4:latest",
          "cloudera:cloudera-centos-os:7_5:latest",
          "RedHat:RHEL:6.7:latest",
          "RedHat:RHEL:7.2:latest",
          "RedHat:RHEL:7.4:latest",
          "RedHat:RHEL:7.5:latest")
      .defaultDescription("The VM image to deploy. This can be one of the following:<br>" +
          "a) (recommended) an Image URN in this format: &lt;publisher&gt;:&lt;offer&gt;:&lt;sku&gt;:&lt;version&gt; " +
          "href='https://docs.microsoft.com/en-us/azure/virtual-machines/linux/cli-ps-findimage#terminology'>" +
          "more information</a>;<br>" +
          "b) an image alias referencing an image in the configurable images file;<br>" +
          "c) (deprecated) an inline image description in the format: " +
          "/publisher/&lt;publisher&gt;/offer/&lt;offer&gt;/sku/&lt;sku&gt;/version/&lt;version&gt;.")
      .defaultPlaceholder("Select VM image alias")
      .defaultErrorMessage("VM Image Alias is required.")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .required(true)
      .build()),

  VMSIZE(new SimpleConfigurationPropertyBuilder()
      .configKey(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.TYPE
          .unwrap().getConfigKey())
      .name("Virtual Machine Size")
      .addValidValues(
          "STANDARD_DS15_V2",
          "STANDARD_DS14_V2",
          "STANDARD_DS13_V2",
          "STANDARD_DS12_V2",
          "STANDARD_D15_V2",
          "STANDARD_D14_V2",
          "STANDARD_D13_V2",
          "STANDARD_D12_V2",
          "STANDARD_DS14",
          "STANDARD_DS13",
          "STANDARD_D14",
          "STANDARD_D13",
          "STANDARD_D64S_V3",
          "STANDARD_D32S_V3",
          "STANDARD_D16S_V3",
          "STANDARD_D8S_V3",
          "STANDARD_E2S_V3",
          "STANDARD_E4S_V3",
          "STANDARD_E8S_V3",
          "STANDARD_E16S_V3",
          "STANDARD_E32S_V3",
          "STANDARD_E64S_V3",
          "STANDARD_GS5",
          "STANDARD_GS4")
      .defaultDescription("The machine type.<br /><a target='_blank' " +
          "href='https://docs.microsoft.com/en-us/azure/virtual-machines/linux/sizes'>More Information</a>")
      .defaultPlaceholder("Select VM size")
      .defaultErrorMessage("Virtual Machine Size is required.")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .required(true)
      .build()),

  COMPUTE_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
      .configKey("computeResourceGroup")
      .name("Compute Resource Group")
      .defaultDescription("The Resource Group where the compute resources such as VM instances " +
          "and availability sets will be created.<br /><a target='_blank' href='https://azure." +
          "microsoft.com/en-us/documentation/articles/resource-group-overview/#resource-groups'>" +
          "More Information</a>")
      .defaultErrorMessage("Resource Group is required.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(true)
      .build()),

  VIRTUAL_NETWORK_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
      .configKey("virtualNetworkResourceGroup")
      .name("Virtual Network Resource Group")
      .defaultDescription("The Resource Group where the Virtual Network is located.")
      .defaultErrorMessage("Resource Group is required.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(true)
      .build()),

  VIRTUAL_NETWORK(new SimpleConfigurationPropertyBuilder()
      .configKey("virtualNetwork")
      .name("Virtual Network")
      .defaultDescription("The Virtual Network for the deployment. This must exist in the " +
          "Virtual Network Resource Group you selected.<br /><a target='_blank' href='https://" +
          "azure.microsoft.com/en-us/documentation/articles/virtual-networks-overview/'>More " +
          "Information</a>")
      .defaultErrorMessage("Virtual Network is required.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(true)
      .build()),

  SUBNET_NAME(new SimpleConfigurationPropertyBuilder()
      .configKey("subnetName")
      .name("Subnet Name")
      .defaultValue("default")
      .defaultDescription("The name of the subnet to use for VMs. This subnet resource must be " +
          "under the virtual network resource you specified.")
      .defaultErrorMessage("Subnet Name is required.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(true)
      .build()),

  HOST_FQDN_SUFFIX(new SimpleConfigurationPropertyBuilder()
      .configKey("hostFqdnSuffix")
      .name("Host FQDN suffix")
      .defaultDescription("The private domain name used to create a FQDN for each host. </ br>" +
          "Note: Azure provided DNS service does not support reverse DNS lookup for private IP " +
          "addresses. You must setup a dedicated DNS service with reverse lookup support and use " +
          "this FQDN Suffix.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(false)
      .build()),

  NETWORK_SECURITY_GROUP_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
      .configKey("networkSecurityGroupResourceGroup")
      .name("Network Security Group Resource Group")
      .defaultDescription("The Resource Group where the Network Security Group is located.")
      .defaultErrorMessage("Resource Group is required.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(true)
      .build()),

  NETWORK_SECURITY_GROUP(new SimpleConfigurationPropertyBuilder()
      .configKey("networkSecurityGroup")
      .name("Network Security Group")
      .defaultDescription("The Network Security Group for the deployment. This must exist in the " +
          "Network Security Group Resource Group you selected.<br /><a target='_blank' " +
          "href='https://azure.microsoft.com/en-us/documentation/articles/virtual-networks-nsg/'>" +
          "More Information</a>")
      .defaultErrorMessage("Network Security Group is required.")
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
      .defaultDescription("The Availability Set for the deployment. This must exist in the " +
          "Compute Resource Group you specified earlier.<br /><a target='_blank' " +
          "href='https://azure.microsoft.com/en-us/documentation/articles/" +
          "virtual-machines-windows-manage-availability/'>More Information</a>")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(false)
      .build()),

  MANAGED_DISKS(new SimpleConfigurationPropertyBuilder()
      .configKey("managedDisks")
      .name("Use Managed Disks")
      .addValidValues(
          "Yes",
          "No")
      .defaultValue("Yes")
      .defaultDescription("Whether to use Managed Disks. Managed Disks are enabled by default " +
          "and he Availability Set must be configured to use Managed Disks. " +
          "To use Storage Accounts set this field to \"No\".<br /><a target='_blank' " +
          "href='https://docs.microsoft.com/en-us/azure/storage/storage-managed-disks-overview'>" +
          "More Information</a>")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .required(false)
      .hidden(true)
      .build()),

  STORAGE_TYPE(new SimpleConfigurationPropertyBuilder()
      .configKey("storageAccountType")
      .name("Storage Type")
      .addValidValues(
          SkuName.PREMIUM_LRS.toString(),
          SkuName.STANDARD_LRS.toString())
      .defaultDescription("The Storage Type to use.")
      .defaultValue(Configurations.AZURE_DEFAULT_STORAGE_TYPE)
      .defaultErrorMessage("Storage Type is required.")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .required(false)
      .build()),

  DATA_DISK_COUNT(new SimpleConfigurationPropertyBuilder()
      .configKey("dataDiskCount")
      .name("Data Disk Count")
      .defaultDescription("The number of data disks to create.")
      .defaultValue("5")
      .type(Property.Type.INTEGER)
      .defaultErrorMessage("Data Disk Count is required.")
      .widget(ConfigurationProperty.Widget.NUMBER)
      .required(false)
      .build()),

  DATA_DISK_SIZE(new SimpleConfigurationPropertyBuilder()
      .configKey("dataDiskSize")
      .name("Data Disk Size in GiB")
      .addValidValues(
          "4095",
          "2048",
          "1024",
          "512")
      .defaultDescription("The size of the data disks in GiB.<br />P/S 50: 4095<br />" +
          "P/S 40: 2048<br />P/S 30: 1024GiB<br />P/S 20: 512GiB<br /><a target='_blank' " +
          "href='https://azure.microsoft.com/en-us/documentation/articles" +
          "/storage-premium-storage/'>More Information</a>")
      .defaultValue("1024")
      .type(Property.Type.INTEGER)
      .defaultErrorMessage("Data Disk Size is required.")
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .required(false)
      .build()),

  USE_CUSTOM_MANAGED_IMAGE(new SimpleConfigurationPropertyBuilder()
      .configKey("useCustomManagedImage")
      .name("Use Custom Managed VM Image")
      .addValidValues(
          "Yes",
          "No")
      .defaultValue("No")
      .defaultDescription("Whether to use custom VM image. Custom image option is only supported " +
          "when using Managed Disks.")
      .widget(ConfigurationProperty.Widget.LIST)
      .required(false)
      .build()),

  CUSTOM_IMAGE_PLAN(new SimpleConfigurationPropertyBuilder()
      .configKey("customImagePlan")
      .name("Custom VM Image purchase plan")
      .defaultDescription("Purchase plan for the original VM image used to create the custom " +
          "image. This can be <empty string> or string with format: " +
          "/publisher/&lt;publisher&gt;/product/&lt;product&gt;/name/&lt;name&gt;.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(false)
      .build()),

  USER_ASSIGNED_MSI_RESOURCE_GROUP(new SimpleConfigurationPropertyBuilder()
      .configKey("userAssignedMsiResourceGroup")
      .name("The User Assigned MSI Resource Group")
      .defaultDescription("The Resource Group that holds the specified User Assigned Managed Service Identity. " +
          "If this and the User Assigned MSI Name field is left blank no MSI will be assigned to the VM.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(false)
      .hidden(true)
      .build()),

  USER_ASSIGNED_MSI_NAME(new SimpleConfigurationPropertyBuilder()
      .configKey("userAssignedMsiName")
      .name("The User Assigned MSI Name")
      .defaultDescription("The User Assigned Managed Service Identity Name. If this and the User Assigned MSI " +
          "Resource Group field is left blank no MSI will be assigned to the VM.")
      .widget(ConfigurationProperty.Widget.TEXT)
      .required(false)
      .hidden(true)
      .build()),

  CUSTOM_DATA_UNENCODED(new SimpleConfigurationPropertyBuilder()
      .configKey("customDataUnencoded")
      .name("Custom Data (unencoded)")
      .defaultDescription("The Custom Data, unencoded. Custom data in this field will be " +
          "Base64 encoded and the encoded string has a maximum length of 87380 characters.")
      .required(false)
      .sensitive(true)
      .hidden(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  CUSTOM_DATA_ENCODED(new SimpleConfigurationPropertyBuilder()
      .configKey("customDataEncoded")
      .name("Custom Data (encoded)")
      .defaultDescription("The Custom Data, Base64 encoded. Custom data in this field must be " +
          "Base64 encoded with a maximum length of 87380 characters.")
      .required(false)
      .sensitive(true)
      .hidden(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  WITH_STATIC_PRIVATE_IP_ADDRESS(new SimpleConfigurationPropertyBuilder()
      .configKey("withStaticPrivateIpAddress")
      .name("With Static Private IP Address")
      .defaultDescription("Whether or not to set the Private IP Address to static.")
      .addValidValues(
          "Yes",
          "No")
      .defaultValue("Yes")
      .widget(ConfigurationProperty.Widget.LIST)
      .required(false)
      .hidden(true)
      .build()),

  WITH_ACCELERATED_NETWORKING(new SimpleConfigurationPropertyBuilder()
      .configKey("withAcceleratedNetworking")
      .name("With Accelerated Networking")
      .defaultDescription("Whether or not to enable Accelerated Networking.")
      .addValidValues(
          "Yes",
          "No")
      .defaultValue("No")
      .widget(ConfigurationProperty.Widget.LIST)
      .required(false)
      .hidden(true)
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
