/*
 * Copyright (c) 2018 Cloudera, Inc.
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
package com.cloudera.director.azure;

import static com.cloudera.director.azure.Configurations.AZURE_DEFAULT_USER_AGENT_GUID;
import static com.cloudera.director.azure.TestHelper.TEST_CENTOS_IMAGE_NAME;
import static com.cloudera.director.azure.TestHelper.TEST_DATA_DISK_SIZE;
import static com.cloudera.director.azure.TestHelper.TEST_EMPTY_CONFIG;
import static com.cloudera.director.azure.TestHelper.TEST_NETWORK_SECURITY_GROUP;
import static com.cloudera.director.azure.TestHelper.TEST_REGION;
import static com.cloudera.director.azure.TestHelper.TEST_RESOURCE_GROUP;
import static com.cloudera.director.azure.TestHelper.TEST_SSH_PUBLIC_KEY;
import static com.cloudera.director.azure.TestHelper.TEST_SUBNET;
import static com.cloudera.director.azure.TestHelper.TEST_VIRTUAL_NETWORK;
import static com.cloudera.director.azure.TestHelper.TEST_VM_SIZE;

import com.cloudera.director.azure.compute.credentials.AzureCloudEnvironment;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationProperty;
import com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v2.model.InstanceTemplate;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.util.Strings;

/**
 * Helper class for managing the various Live Test scenarios.
 */
public class AzureCreator {
  private static final String YES = "Yes";
  private static final String NO = "No";

  public static class Builder {
    private int numberOfVMs = 1;
    private String subscriptionId =
        System.getProperty(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey());
    private String tenantId =
        System.getProperty(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey());
    private String clientId =
        System.getProperty(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey());
    private String clientSecret =
        System.getProperty(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey());
    private String userAgent = AZURE_DEFAULT_USER_AGENT_GUID;
    private String instanceNamePrefix = "director";
    private String image = TEST_CENTOS_IMAGE_NAME;
    private String vmSize = TEST_VM_SIZE;
    private boolean useVmss = false;
    private String sshUsername = "cloudera";
    private String sshOpensshPublicKey = TEST_SSH_PUBLIC_KEY;
    private String region = TEST_REGION;
    private String computeResourceGroup = TEST_RESOURCE_GROUP;
    private String virtualNetworkResourceGroup = TEST_RESOURCE_GROUP;
    private String virtualNetwork = TEST_VIRTUAL_NETWORK;
    private String subnetName = TEST_SUBNET;
    private String hostFqdnSuffix = TestHelper.TEST_HOST_FQDN_SUFFIX;
    private String networkSecurityGroupResourceGroup = TEST_RESOURCE_GROUP;
    private String networkSecurityGroup = TEST_NETWORK_SECURITY_GROUP;
    private String publicIP = YES;
    private String availabilitySet = TEST_EMPTY_CONFIG;
    private String managedDisks = YES;
    private String storageAccountType = SkuName.STANDARD_LRS.toString();
    private String dataDiskCount = "1";
    private String dataDiskSize = TEST_DATA_DISK_SIZE;
    private String useCustomManagedImage = TEST_EMPTY_CONFIG;
    private String customImagePlan = TEST_EMPTY_CONFIG;
    private String userAssignedMsiName = TEST_EMPTY_CONFIG;
    private String userAssignedMsiResourceGroup = TEST_EMPTY_CONFIG;
    private String useImplicitMsi = TEST_EMPTY_CONFIG;
    private String implicitMsiAadGroupName = TEST_EMPTY_CONFIG;
    private String withStaticPrivateIpAddress = YES;
    private String withAcceleratedNetworking = NO;
    private String customDataEncoded = TEST_EMPTY_CONFIG;
    private String customDataUnencoded = TEST_EMPTY_CONFIG;

    public Builder setNumberOfVMs(int numberOfVMs) {
      this.numberOfVMs = numberOfVMs;
      return this;
    }

    public Builder setSubscriptionId(String subscriptionId) {
      this.subscriptionId = subscriptionId;
      return this;
    }

    public Builder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public Builder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setClientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public Builder setUserAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder setUseVmss(boolean useVmss) {
      this.useVmss = useVmss;
      return this;
    }

    public Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public Builder setInstanceNamePrefix(String instanceNamePrefix) {
      this.instanceNamePrefix = instanceNamePrefix;
      return this;
    }

    public Builder setSshUsername(String sshUsername) {
      this.sshUsername = sshUsername;
      return this;
    }

    public Builder setSshOpensshPublicKey(String sshOpensshPublicKey) {
      this.sshOpensshPublicKey = sshOpensshPublicKey;
      return this;
    }

    public Builder setImage(String image) {
      this.image = image;
      return this;
    }

    public Builder setVmSize(String vmSize) {
      this.vmSize = vmSize;
      return this;
    }

    public Builder setComputeResourceGroup(String computeResourceGroup) {
      this.computeResourceGroup = computeResourceGroup;
      return this;
    }

    public Builder setVirtualNetworkResourceGroup(String virtualNetworkResourceGroup) {
      this.virtualNetworkResourceGroup = virtualNetworkResourceGroup;
      return this;
    }

    public Builder setVirtualNetwork(String virtualNetwork) {
      this.virtualNetwork = virtualNetwork;
      return this;
    }

    public Builder setSubnetName(String subnetName) {
      this.subnetName = subnetName;
      return this;
    }

    public Builder setHostFqdnSuffix(String hostFqdnSuffix) {
      this.hostFqdnSuffix = hostFqdnSuffix;
      return this;
    }

    public Builder setNetworkSecurityGroupResourceGroup(String networkSecurityGroupResourceGroup) {
      this.networkSecurityGroupResourceGroup = networkSecurityGroupResourceGroup;
      return this;
    }

    public Builder setNetworkSecurityGroup(String networkSecurityGroup) {
      this.networkSecurityGroup = networkSecurityGroup;
      return this;
    }

    public Builder setPublicIP(String publicIP) {
      this.publicIP = publicIP;
      return this;
    }

    public Builder setPublicIP(boolean publicIP) {
      this.publicIP = publicIP ? YES : NO;
      return this;
    }

    public Builder setAvailabilitySet(String availabilitySet) {
      this.availabilitySet = availabilitySet;
      return this;
    }

    public Builder setManagedDisks(String managedDisks) {
      this.managedDisks = managedDisks;
      return this;
    }

    public Builder setManagedDisks(boolean managedDisks) {
      this.managedDisks = managedDisks ? YES : NO;
      return this;
    }

    public Builder setStorageAccountType(String storageAccountType) {
      this.storageAccountType = storageAccountType;
      return this;
    }

    public Builder setDataDiskCount(String dataDiskCount) {
      this.dataDiskCount = dataDiskCount;
      return this;
    }

    public Builder setDataDiskCount(int dataDiskCount) {
      this.dataDiskCount = Integer.toString(dataDiskCount);
      return this;
    }

    public Builder setDataDiskSize(String dataDiskSize) {
      this.dataDiskSize = dataDiskSize;
      return this;
    }

    public Builder setDataDiskSize(int dataDiskSize) {
      this.dataDiskSize = Integer.toString(dataDiskSize);
      return this;
    }

    public Builder setUseCustomManagedImage(String useCustomManagedImage) {
      this.useCustomManagedImage = useCustomManagedImage;
      return this;
    }

    public Builder setCustomImagePlan(String customImagePlan) {
      this.customImagePlan = customImagePlan;
      return this;
    }

    public Builder setUserAssignedMsiResourceGroup(String userAssignedMsiResourceGroup) {
      this.userAssignedMsiResourceGroup = userAssignedMsiResourceGroup;
      return this;
    }

    public Builder setUserAssignedMsiName(String userAssignedMsiName) {
      this.userAssignedMsiName = userAssignedMsiName;
      return this;
    }

    public Builder setUseImplicitMsi(String useImplicitMsi) {
      this.useImplicitMsi = useImplicitMsi;
      return this;
    }

    public Builder setImplicitMsiAadGroupName(String implicitMsiAadGroupName) {
      this.implicitMsiAadGroupName = implicitMsiAadGroupName;
      return this;
    }

    public Builder setCustomDataUnecoded(String customDataUnencoded) {
      this.customDataUnencoded = customDataUnencoded;
      return this;
    }

    public Builder setCustomDataEncoded(String customDataEncoded) {
      this.customDataEncoded = customDataEncoded;
      return this;
    }

    public Builder setWithStaticPrivateIpAddress(String withStaticPrivateIpAddress) {
      this.withStaticPrivateIpAddress = withStaticPrivateIpAddress;
      return this;
    }

    public Builder setWithStaticPrivateIpAddress(boolean withStaticPrivateIpAddress) {
      this.withStaticPrivateIpAddress = withStaticPrivateIpAddress ? YES : NO;
      return this;
    }

    public Builder setWithAcceleratedNetworking(String withAcceleratedNetworking) {
      this.withAcceleratedNetworking = withAcceleratedNetworking;
      return this;
    }

    public Builder setWithAcceleratedNetworking(boolean withAcceleratedNetworking) {
      this.withAcceleratedNetworking = withAcceleratedNetworking ? YES : NO;
      return this;
    }

    public AzureCreator build() {
      return new AzureCreator(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private int numberOfVMs;
  private String subscriptionId;
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private String userAgent;
  private boolean useVmss;
  private String groupId;
  private String region;
  private String instanceNamePrefix;
  private String sshUsername;
  private String sshOpensshPublicKey;
  private String image;
  private String vmSize;
  private String computeResourceGroup;
  private String virtualNetworkResourceGroup;
  private String virtualNetwork;
  private String subnetName;
  private String hostFqdnSuffix;
  private String networkSecurityGroupResourceGroup;
  private String networkSecurityGroup;
  private String publicIP;
  private String availabilitySet;
  private String managedDisks;
  private String storageAccountType;
  private String dataDiskCount;
  private String dataDiskSize;
  private String useCustomManagedImage;
  private String customImagePlan;
  private String userAssignedMsiResourceGroup;
  private String userAssignedMsiName;
  private String useImplicitMsi;
  private String implicitMsiAadGroupName;
  private String customDataUnencoded;
  private String customDataEncoded;
  private String withStaticPrivateIpAddress;
  private String withAcceleratedNetworking;

  private AzureCreator(Builder builder) {
    this.numberOfVMs = builder.numberOfVMs;
    this.subscriptionId = builder.subscriptionId;
    this.tenantId = builder.tenantId;
    this.clientId = builder.clientId;
    this.clientSecret = builder.clientSecret;
    this.userAgent = builder.userAgent;
    this.useVmss = builder.useVmss;
    if (this.useVmss) {
      this.groupId = RandomStringUtils.randomAlphabetic(8).toLowerCase();
    }
    this.region = builder.region;
    this.instanceNamePrefix = builder.instanceNamePrefix;
    this.sshUsername = builder.sshUsername;
    this.sshOpensshPublicKey = builder.sshOpensshPublicKey;
    this.image = builder.image;
    this.vmSize = builder.vmSize;
    this.computeResourceGroup = builder.computeResourceGroup;
    this.virtualNetworkResourceGroup = builder.virtualNetworkResourceGroup;
    this.virtualNetwork = builder.virtualNetwork;
    this.subnetName = builder.subnetName;
    this.hostFqdnSuffix = builder.hostFqdnSuffix;
    this.networkSecurityGroupResourceGroup = builder.networkSecurityGroupResourceGroup;
    this.networkSecurityGroup = builder.networkSecurityGroup;
    this.publicIP = builder.publicIP;
    this.availabilitySet = builder.availabilitySet;
    this.managedDisks = builder.managedDisks;
    this.storageAccountType = builder.storageAccountType;
    this.dataDiskCount = builder.dataDiskCount;
    this.dataDiskSize = builder.dataDiskSize;
    this.useCustomManagedImage = builder.useCustomManagedImage;
    this.customImagePlan = builder.customImagePlan;
    this.userAssignedMsiResourceGroup = builder.userAssignedMsiResourceGroup;
    this.userAssignedMsiName = builder.userAssignedMsiName;
    this.useImplicitMsi = builder.useImplicitMsi;
    this.implicitMsiAadGroupName = builder.implicitMsiAadGroupName;
    this.customDataUnencoded = builder.customDataUnencoded;
    this.customDataEncoded = builder.customDataEncoded;
    this.withStaticPrivateIpAddress = builder.withStaticPrivateIpAddress;
    this.withAcceleratedNetworking = builder.withAcceleratedNetworking;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public int getNumberOfVMs() {
    return numberOfVMs;
  }

  public boolean useVmss() {
    return useVmss;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getRegion() {
    return region;
  }

  public String getInstanceNamePrefix() {
    return instanceNamePrefix;
  }

  public String getVmSize() {
    return vmSize;
  }

  public String getComputeResourceGroup() {
    return computeResourceGroup;
  }

  public String getHostFqdnSuffix() {
    return hostFqdnSuffix;
  }

  public boolean hasHostFqdnSuffix() {
    return !Strings.isNullOrEmpty(hostFqdnSuffix);
  }

  public boolean withPublicIP() {
    return AzureVirtualMachineMetadata.parseYesNo(publicIP);
  }

  public boolean withManagedDisks() {
    return AzureVirtualMachineMetadata.parseYesNo(managedDisks);
  }

  public String getStorageAccountType() {
    return Configurations.convertStorageAccountTypeString(storageAccountType);
  }

  public int getDataDiskCount() {
    return Integer.parseInt(dataDiskCount);
  }

  public boolean withUserAssignedMsi() {
    return !Strings.isNullOrEmpty(userAssignedMsiName) &&
        !Strings.isNullOrEmpty(userAssignedMsiResourceGroup);
  }

  public boolean withStaticPrivateIpAddress() {
    return AzureVirtualMachineMetadata.parseYesNo(withStaticPrivateIpAddress);
  }

  public boolean withAcceleratedNetworking() {
    return AzureVirtualMachineMetadata.parseYesNo(withAcceleratedNetworking);
  }


  public Map<String, String> createMap() {
    Map<String, String> map = new HashMap<>();

    map.put(AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT.unwrap().getConfigKey(), AzureCloudEnvironment.AZURE);
    map.put(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey(), subscriptionId);
    map.put(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey(), tenantId);
    map.put(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey(), clientId);
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(), clientSecret);
    map.put(AzureCredentialsConfiguration.USER_AGENT.unwrap().getConfigKey(), userAgent);
    if (useVmss) {
      map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.AUTOMATIC.unwrap()
          .getConfigKey(), String.valueOf(true));
      map.put(SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.GROUP_ID.unwrap().getConfigKey(),
          groupId);
    }
    map.put(AzureComputeProviderConfigurationProperty.REGION.unwrap().getConfigKey(), region);
    map.put(InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX.unwrap().getConfigKey(),
        instanceNamePrefix);
    map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME.unwrap()
        .getConfigKey(), sshUsername);
    map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_OPENSSH_PUBLIC_KEY.unwrap()
        .getConfigKey(), sshOpensshPublicKey);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(), image);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE.unwrap().getConfigKey(), vmSize);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap().getConfigKey(),
        computeResourceGroup);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP.unwrap().getConfigKey(),
        virtualNetworkResourceGroup);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK.unwrap().getConfigKey(), virtualNetwork);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME.unwrap().getConfigKey(), subnetName);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX.unwrap().getConfigKey(), hostFqdnSuffix);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP.unwrap()
        .getConfigKey(), networkSecurityGroupResourceGroup);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP.unwrap().getConfigKey(),
        networkSecurityGroup);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey(), publicIP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap().getConfigKey(),
        availabilitySet);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(), managedDisks);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap().getConfigKey(), storageAccountType);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap().getConfigKey(), dataDiskCount);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE.unwrap().getConfigKey(), dataDiskSize);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap().getConfigKey(),
        useCustomManagedImage);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap().getConfigKey(),
        customImagePlan);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP.unwrap().getConfigKey(),
        userAssignedMsiResourceGroup);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME.unwrap().getConfigKey(),
        userAssignedMsiName);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap().getConfigKey(),
        useImplicitMsi);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME.unwrap().getConfigKey(),
        implicitMsiAadGroupName);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_UNENCODED.unwrap().getConfigKey(),
        customDataUnencoded);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_ENCODED.unwrap().getConfigKey(),
        customDataEncoded);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.WITH_STATIC_PRIVATE_IP_ADDRESS.unwrap().getConfigKey(),
        withStaticPrivateIpAddress);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.WITH_ACCELERATED_NETWORKING.unwrap().getConfigKey(),
        withAcceleratedNetworking);

    return map;
  }
}
