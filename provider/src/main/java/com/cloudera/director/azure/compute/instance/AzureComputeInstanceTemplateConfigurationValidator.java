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

import static com.cloudera.director.spi.v1.model.util.Validations.addError;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceTemplate;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.exception.ValidationException;
import com.google.common.collect.Sets;
import com.microsoft.azure.CloudException;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.AvailabilitySet;
import com.microsoft.azure.management.compute.AvailabilitySetSkuTypes;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.compute.VirtualMachineCustomImage;
import com.microsoft.azure.management.compute.VirtualMachineSize;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryGroup;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.microsoft.azure.management.msi.Identity;
import com.microsoft.azure.management.msi.implementation.MSIManager;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.storage.SkuName;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validator for Azure Compute Instance configs. Entry point is the validate method.
 *
 * Director does initial config validation to verify that all required fields exist and are the
 * correct type.
 */
public class AzureComputeInstanceTemplateConfigurationValidator implements ConfigurationValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(AzureComputeInstanceTemplateConfigurationValidator.class);

  // Credential to do Azure specific validations
  private AzureCredentials credentials;

  // The region represents the data center; some resource may be valid in one data center but not
  // in others.
  private String region;

  /**
   * Builds a validator.
   *
   * @param credentials credential object to get helper for Azure SDK calls
   * @param region the deployment region
   */
  public AzureComputeInstanceTemplateConfigurationValidator(AzureCredentials credentials,
      String region) {
    this.credentials = credentials;
    this.region = region;
  }

  /**
   * Validates template configuration including checks that rely on the Azure backend. Before
   * calling this method Director has already done initial config validation to verify that all
   * required fields exist and are the correct type.
   *
   * @param name not used
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  @Override
  public void validate(String name, Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    final String genericErrorMsg = "Error occurred during validating: %s";

    // Checks that don't reach out to the Azure backend.
    checkFQDNSuffix(directorConfig, accumulator, localizationContext);
    checkInstancePrefix(directorConfig, accumulator, localizationContext);
    checkStorage(directorConfig, accumulator, localizationContext);
    checkSshUsername(directorConfig, accumulator, localizationContext);

    // Azure backend checks: These checks verifies the resources specified in instance template do
    // exist in Azure.
    try {
      Azure azure = credentials.authenticate();

      checkComputeResourceGroup(directorConfig, accumulator, localizationContext, azure);
      checkNetwork(directorConfig, accumulator, localizationContext, azure);
      checkNetworkSecurityGroupResourceGroup(directorConfig, accumulator, localizationContext,
          azure);
      checkNetworkSecurityGroup(directorConfig, accumulator, localizationContext, azure);
      checkAvailabilitySetAndManagedDisks(directorConfig, accumulator, localizationContext, azure);
      checkVmImage(directorConfig, accumulator, localizationContext, azure);
      checkUseCustomImage(directorConfig, accumulator, localizationContext, azure);
      checkUserAssignedMsi(directorConfig, accumulator, localizationContext);
      checkImplicitMsiGroupName(directorConfig, accumulator, localizationContext);
    } catch (Exception e) {
      LOG.debug(genericErrorMsg, e);
      // use null key to indicate generic error
      ConfigurationPropertyToken token = null;
      addError(accumulator, token, localizationContext, null, genericErrorMsg, e);
    }
  }

  /**
   * Checks to make sure FQDN suffix satisfies Azure DNS label and hostname requirements.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkFQDNSuffix(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    final String fqdnSuffixMsg = "FQDN suffix '%s' does not satisfy Azure DNS name suffix " +
        "requirement: '%s'.";

    String suffix = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX, localizationContext);

    // if no fqdn suffix was specified then short-circuit
    if (suffix == null || suffix.trim().isEmpty()) {
      // short-circuit return
      return;
    }

    String regex = AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
        .getString(Configurations.AZURE_CONFIG_INSTANCE_FQDN_SUFFIX_REGEX);
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(suffix);

    if (!matcher.find()) {
      LOG.debug(String.format(fqdnSuffixMsg, suffix, regex));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX,
          localizationContext, null, fqdnSuffixMsg, suffix, regex);
    }
  }

  /**
   * Checks to make sure instance name prefix satisfies Azure DNS label requirements.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkInstancePrefix(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext) {
    final String instanceNamePrefixMsg =
        "Instance name prefix '%s' does not satisfy Azure DNS label requirement: %s.";

    String instancePrefix = directorConfig.getConfigurationValue(
        InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX,
        localizationContext);
    String regex = AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
        .getString(Configurations.AZURE_CONFIG_INSTANCE_PREFIX_REGEX);
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(instancePrefix);

    if (!matcher.find()) {
      LOG.debug(String.format(instanceNamePrefixMsg, instancePrefix, regex));
      addError(accumulator,
          InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX,
          localizationContext, null, instanceNamePrefixMsg, instancePrefix, regex);
    }
  }

  /**
   * Checks that the Storage Account Type and the Data Disk Size are supported.
   *
   * By default the following Account Types are supported:
   * - Premium_LRS
   * - Standard_LRS
   *
   * By default the following Premium_LRS Data Disk Sizes are supported:
   * - P20: 512
   * - P30: 1024
   * - P40: 2048
   * - P50: 4095
   * AZURE_SDK:
   * P50 does not map to 4096 because 'dataDisk.diskSizeGB' must be between 1 and 4095 inclusive
   * otherwise Azure will throw a ServiceException.
   * See https://azure.microsoft.com/en-us/documentation/articles/storage-premium-storage/
   *
   * By default the following are the restrictions for Disk Sizes:
   * - Minimum: >= 1 GB
   * - Maximum: <= 4095
   * AZURE_SDK:
   * The limit is not 4096 because 'dataDisk.diskSizeGB' must be between 1 and 4095 inclusive for
   * Standard Storage disks.
   *
   * Non-default Account Types are not subject to any restrictions and are assumed to be correct.
   * Azure, however, will still throw an exception down the line if the values are incorrect.
   *
   * If a value is used that falls outside the range of acceptable values this exception will be
   * thrown (for both Standard and Premium disks):
   * ServiceException: InvalidParameter: The value '4096' of parameter 'dataDisk.diskSizeGB' is
   * out of range. Value '4096' must be between '1' and '4095' inclusive.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkStorage(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    final String invalidStorageAccountTypeMsg = "Storage Account Type '%s' is not a valid Azure " +
        "Storage Account Type. Valid types: '%s'";
    final String unsupportedStorageAccountTypesMsg = "Storage Account Type '%s' is not " +
        "supported. Supported types: %s.";
    final String diskSizeLessThanMinMsg = "Disk size %s is invalid. It must be greater than 0.";
    final String diskSizeGreaterThanMaxMsg = "Disk size %s is invalid. It must be less than or equal to %s.";
    final String noDiskValidationsMsg = "Disk size is not validated for storage type '%s'.";

    // first validate the storage account type
    String storageAccountType = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
        localizationContext);

    // change the deprecated storage account type to current
    storageAccountType = Configurations.convertStorageAccountTypeString(storageAccountType);

    // validate that the storage account type (Premium_LRS by default) is a valid AccountType enum
    if (SkuName.fromString(storageAccountType) == null) {
      // logging for current storage
      LOG.debug(String.format(invalidStorageAccountTypeMsg, storageAccountType,
          Arrays.asList(SkuName.values())));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
          localizationContext, null, invalidStorageAccountTypeMsg, storageAccountType,
          Arrays.asList(SkuName.values()));

      // short-circuit return
      return;
    }

    // validate that the storage account type (Premium_LRS by default) exists in azure-plugin.conf
    if (!AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
        .getStringList(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES)
        .contains(storageAccountType)) {
      LOG.debug(String.format(unsupportedStorageAccountTypesMsg, storageAccountType,
          AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
              .getStringList(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES)));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
          localizationContext, null, unsupportedStorageAccountTypesMsg, storageAccountType,
          AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
              .getStringList(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES));

      // short-circuit return
      return;
    }

    // given the storage account type validate the data disk size
    String diskSize = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE, localizationContext);

    if (storageAccountType.equals(SkuName.PREMIUM_LRS.toString()) ||
        storageAccountType.equals(SkuName.STANDARD_LRS.toString())) {
      int diskSizeGB = Integer.parseInt(diskSize);
      if (diskSizeGB < 1) {
        LOG.debug(String.format(diskSizeLessThanMinMsg, diskSize));
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE,
            localizationContext, null, diskSizeLessThanMinMsg, diskSize);
      } else if (diskSizeGB > Integer.parseInt(AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
          .getString(Configurations.AZURE_CONFIG_INSTANCE_MAXIMUM_DISK_SIZE))) {
        // The min is 1GB, the max is currently 4095 configurable by changing azure-plugin.conf. If
        // the value used falls outside Azure limitations then the VM(s) will not be provisioned and a
        // ServiceException and message will be in Director logs.
        LOG.debug(String.format(diskSizeGreaterThanMaxMsg, diskSize,
            AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
                .getString(Configurations.AZURE_CONFIG_INSTANCE_MAXIMUM_DISK_SIZE)));
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE,
            localizationContext, null, diskSizeGreaterThanMaxMsg, diskSize,
            AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
                .getString(Configurations.AZURE_CONFIG_INSTANCE_MAXIMUM_DISK_SIZE));
      }
    } else {
      // if it's not part of the default list don't do any size validations
      LOG.info(String.format(noDiskValidationsMsg, storageAccountType));
    }
  }

  /**
   * Checks that username is not one of the Azure disallowed user names.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkSshUsername(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    final String disallowedUsernamesMsg = "Username '%s' is not allowed. Disallowed usernames: " +
        "'%s'";

    String sshUsername = directorConfig.getConfigurationValue(
        ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME,
        localizationContext);

    List<String> disallowedUsernames = AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
        .getStringList(Configurations.AZURE_CONFIG_DISALLOWED_USERNAMES);

    if (disallowedUsernames.contains(sshUsername)) {
      LOG.debug(String.format(disallowedUsernamesMsg, sshUsername, disallowedUsernames));
      addError(accumulator,
          ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME,
          localizationContext, null, disallowedUsernamesMsg, sshUsername, disallowedUsernames);
    }
  }

  /**
   * Checks that the Compute Resource Group exists in Azure. This Resource Group defines where
   * VMs and their dependent resources will be provisioned into, and where to look for Availability
   * Sets.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   */
  void checkComputeResourceGroup(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String computeResourceGroupMsg = "Compute Resource Group '%s' does not exist. Create " +
        "the Resource Group or use an existing one.";

    String computeRgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);

    if (azure.resourceGroups().getByName(computeRgName) == null) {
      LOG.debug(String.format(computeResourceGroupMsg, computeRgName));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
          localizationContext, null, computeResourceGroupMsg, computeRgName);
    }
  }

  /**
   * Does these checks in sequence, short circuiting on any failures:
   * 1. The Virtual Network Resource Group exists in Azure.
   * 2. The Virtual Network exists within the Virtual Network Resource Group and is in the
   * environment's region
   * 3. The Subnet exists within the Virtual Network.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   */
  void checkNetwork(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String virtualNetworkResourceGroupMsg = "Virtual Network Resource Group '%s' does not " +
        "exist. Create the Resource Group or use an existing one.";
    final String virtualNetworkMsg = "Virtual Network '%s' does not exist within the Resource " +
        "Group '%s'. Create the Virtual Network or use an existing one.";
    final String virtualNetworkNotInRegionMsg = "Virtual Network '%s' is not in region '%s'. Use " +
        "a virtual network from that region.";
    final String subnetMsg = "Subnet '%s' does not exist under the Virtual Network '%s'. Create " +
        "the subnet or use an existing one.";

    String vnrgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    String vnName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK, localizationContext);
    String subnetName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME,
        localizationContext);

    if (azure.resourceGroups().getByName(vnrgName) == null) {
      LOG.debug(String.format(virtualNetworkResourceGroupMsg, vnrgName));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
          localizationContext, null, virtualNetworkResourceGroupMsg, vnrgName);

      // short circuit return
      return;
    }

    // check that the RG contains the VN
    Network vn = azure.networks().getByResourceGroup(vnrgName, vnName);
    if (vn == null) {
      LOG.debug(String.format(virtualNetworkMsg, vnName, vnrgName));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
          localizationContext, null, virtualNetworkMsg, vnName, vnrgName);

      // short-circuit return
      return;
    }

    // check that the VN is in the environment's region
    if (!vn.regionName().equalsIgnoreCase(region)) {
      LOG.debug(String.format(virtualNetworkNotInRegionMsg, vnName, region));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
          localizationContext, null, virtualNetworkMsg, vnName, region);

      // short-circuit return
      return;
    }

    // check that the VN contains the subnet
    if (!vn.subnets().containsKey(subnetName)) {
      LOG.debug(String.format(subnetMsg, subnetName, vnName));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME,
          localizationContext, null, subnetMsg, subnetName, vnName);
    }
  }

  /**
   * Checks that the Resource Group exists in Azure. This Resource Group defines where to look for
   * the Network Security Group.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   */
  void checkNetworkSecurityGroupResourceGroup(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String networkSecurityGroupResourceGroupMsg = "Network Security Group Resource " +
        "Group '%s' does not exist. Create the Resource Group or use an existing one.";

    String nsgrgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);

    if (azure.resourceGroups().getByName(nsgrgName) == null) {
      LOG.debug(String.format(networkSecurityGroupResourceGroupMsg, nsgrgName));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
          localizationContext, null, networkSecurityGroupResourceGroupMsg, nsgrgName);
    }
  }

  /**
   * Checks that the Network Security Group exists within the Resource Group.
   *  @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure   */
  void checkNetworkSecurityGroup(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String networkSecurityGroupNotInRGMsg = "Network Security Group '%s' does not exist " +
        "within the Resource Group '%s'. Create the Network Security Group or use an existing one.";
    final String networkSecurityGroupNotInRegionMsg = "Network Security Group '%s' is not in " +
        "region '%s'. Use a network security group set from that region.";

    String nsgrgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    String nsgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        localizationContext);

    // Check that the NSG is in the RG
    if (azure.networkSecurityGroups().getByResourceGroup(nsgrgName, nsgName) == null) {
      LOG.debug(String.format(networkSecurityGroupNotInRGMsg, nsgName, nsgrgName));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
          localizationContext, null, networkSecurityGroupNotInRGMsg, nsgName, nsgrgName);

      // short-circuit return
      return;
    }

    // Check that the NSG is in the region
    if (!azure.networkSecurityGroups().getByResourceGroup(nsgrgName, nsgName).regionName()
        .equals(region)) {
      LOG.debug(String.format(networkSecurityGroupNotInRegionMsg, nsgName, region));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
          localizationContext, null, networkSecurityGroupNotInRegionMsg, nsgName, region);
    }
  }

  /**
   * Checks that the Availability Set exists in Azure and that the Availability Set supports the
   * version of VM being created. Different versions of the same VM size (e.g. v2 vs. non-v2) and
   * storage type (e.g. managed-disks vs. non-managed-disks) cannot coexist in the same Availability
   * Set.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   */
  void checkAvailabilitySetAndManagedDisks(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String availabilitySetNotInRGMsg = "Availability Set '%s' does not exist in Compute " +
        "Resource Group '%s'. Create the Availability Set or use an existing one.";
    final String availabilitySetNotInRegionMsg = "Availability Set '%s' is not in region '%s'. " +
        "Use an availability set from that region.";
    final String availabilitySetVmMismatchMsg = "Virtual Machine size '%s' is not allowed in " +
        "Availability Set '%s'. Different versions of the same VM size (e.g. v2 vs. non-v2) " +
        "cannot coexist in the same Availability Set. An Availability Set should be dedicated to " +
        "a specific cluster and node type. Use a different Availability Set or use a VM size " +
        "from this list: '%s'";
    final String availabilitySetStorageAccountsMismatchMsg = "Managed Availability Set '%s' does " +
        "not allow VMs with unmanaged disks. Use an unmanaged Availability Set or use Managed " +
        "Disks for the VM.";
    final String availabilitySetManagedDisksMismatchMsg = "Unmanaged Availability Set '%s' does " +
        "not allow VMs with managed disks. Use a managed Availability Set or use Unmanaged Disks " +
        "(Storage Accounts) for the VM.";

    String computeRgName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    String asName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET, localizationContext);

    // If no AS was specified then short-circuit
    if (asName == null || asName.trim().isEmpty()) {
      // short-circuit return
      LOG.debug("No Availability Set specified, skipping AS checks.");
      return;
    }

    // Check that the AS is in the RG
    AvailabilitySet as = azure.availabilitySets().getByResourceGroup(computeRgName, asName);
    if (as == null) {
      LOG.debug(String.format(availabilitySetNotInRGMsg, asName, computeRgName));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
          localizationContext, null, availabilitySetNotInRGMsg, asName, computeRgName);

      // short-circuit return
      return;
    }

    // Check that the AS is in the region
    if (!as.regionName().equals(region)) {
      LOG.debug(String.format(availabilitySetNotInRegionMsg, asName, region));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
          localizationContext, null, availabilitySetNotInRegionMsg, asName, region);
      return;
    }

    boolean passesVmSizeRegionCheck =
        checkVMSizeForRegion(directorConfig, accumulator, localizationContext, azure);

    if (passesVmSizeRegionCheck) {
      // Check that the AS supports the family of VM being created
      String vmSize = directorConfig
          .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE, localizationContext)
          .toUpperCase();
      // Normalize the Azure-supported list of VM Sizes (pulled from Azure backend)
      Set<String> azureSupportedVmSizes = new HashSet<>();
      for (VirtualMachineSize supportedVMSize : as.listVirtualMachineSizes()) {
        azureSupportedVmSizes.add(supportedVMSize.name().toUpperCase());
      }

      if (!azureSupportedVmSizes.contains(vmSize)) {
        LOG.debug(String.format(availabilitySetVmMismatchMsg, vmSize, asName, azureSupportedVmSizes));
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
            localizationContext, null, availabilitySetVmMismatchMsg, vmSize, asName, azureSupportedVmSizes);
      }
    }

    // Check that the AS and MD/SA choices are compatible
    AvailabilitySetSkuTypes selectedDiskType = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS, localizationContext)
        .equals("Yes") ? AvailabilitySetSkuTypes.MANAGED : AvailabilitySetSkuTypes.UNMANAGED;
    AvailabilitySetSkuTypes asDiskType = as.sku();

    if (!selectedDiskType.equals(asDiskType)) {
      // Propagate the right error
      if (selectedDiskType == AvailabilitySetSkuTypes.MANAGED) {
        // Managed Disks selected, but AS supports Storage Accounts
        LOG.debug(String.format(availabilitySetManagedDisksMismatchMsg, asName));
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
            localizationContext, null, availabilitySetManagedDisksMismatchMsg, asName);
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
            localizationContext, null, availabilitySetManagedDisksMismatchMsg, asName);
      } else {
        // Storage Accounts selected, but AS supports Managed Disks
        LOG.debug(String.format(availabilitySetStorageAccountsMismatchMsg, asName));
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
            localizationContext, null, availabilitySetStorageAccountsMismatchMsg, asName);
        addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
            localizationContext, null, availabilitySetStorageAccountsMismatchMsg, asName);
      }
    }
  }

  /**
   * Checks that VM image string is either:
   * a. a URI one line representation of an image in this format:
   *    /publisher/<publisher>/offer/<offer>/sku/<sku>/version/<version>
   * b. otherwise, an image specified in the configurable images file
   *
   * If it's a valid image definition then check that the image exists in the Azure Marketplace.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   */
  void checkVmImage(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext, Azure azure) {
    final String imageMissingInAzureMsg = "Image with region; %s; publisher: %s; offer: %s; " +
        "sku: %s; and version: %s; does not exist in Azure.";
    final String imageInvalidMsg = "Image with region: %s; publisher: %s; offer: %s; sku: %s; " +
        "and version: %s; is not a valid image.";
    final boolean useCustomImage = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
        localizationContext).equals("Yes");

    // skip VM image check if user is using custom image
    if (useCustomImage) {
      return;
    }

    String imageString = directorConfig.getConfigurationValue(
        ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
        localizationContext);

    ImageReference image;
    String publisher;
    String offer;
    String sku;
    String version;
    try {
      image = Configurations.parseImageFromConfig(directorConfig, localizationContext);

      publisher = image.publisher();
      offer = image.offer();
      sku = image.sku();
      version = image.version();

    } catch (ValidationException e) {
      LOG.debug(e.getMessage());
      addError(accumulator,
          ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
          localizationContext, null, e.getMessage());

      // short circuit return
      return;
    }

    // FIXME preview image validation (the offer ends with "-preview") - CURRENTLY NO VALIDATION IS DONE
    // There's a bug in preview image validation: https://github.com/Azure/azure-sdk-for-java/issues/1890
    if (Configurations.isPreviewImage(image)) {
      LOG.info("Image '{}' is a preview image. Publisher: {}; offer: {}; sku: {}; version: {}.",
          imageString, publisher, offer, sku  , version);

      // short circuit return
      return;
    }

    // regular image validation
    try {
      if (azure.virtualMachineImages().getImage(region, publisher, offer, sku, version) == null) {
        LOG.debug(String.format(imageMissingInAzureMsg, region, publisher, offer, sku, version));
        addError(accumulator,
            ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
            localizationContext, null, imageMissingInAzureMsg, region, publisher, offer, sku,
            version);
      }
    } catch (CloudException e) {
      LOG.debug(String.format(imageInvalidMsg, region, publisher, offer, sku, version));
      addError(accumulator,
          ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
          localizationContext, null, imageInvalidMsg, region, publisher, offer, sku, version);
    }
  }

  /**
   * Checks to make sure VM size (type) exists for the Azure region.
   *
   * Can't check with VirtualMachineSizeTypes.values() because it doesn't contain the full
   * set of allowable VM sizes, so this must be done with a backend call.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   * @param azure the entry point for accessing resource management APIs in Azure
   * @return whether validation was successful
   */
  boolean checkVMSizeForRegion(Configured directorConfig,
                               PluginExceptionConditionAccumulator accumulator,
                               LocalizationContext localizationContext, Azure azure) {
    final String virtualMachineMsg = "Virtual Machine '%s' is not a valid Virtual Machine Size Type in " +
        "region '%s'. Valid Virtual Machine Size Types: %s";

    String vmSize = directorConfig
        .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE, localizationContext);

    boolean isSuccessful = true;

    Set<String> allowableVmSizes = Sets.newHashSet();
    for (VirtualMachineSize vmSizeForRegion :
        azure.virtualMachines().sizes().listByRegion(region)) {
      allowableVmSizes.add(vmSizeForRegion.name().toUpperCase());
    }

    if (!allowableVmSizes.contains(vmSize.toUpperCase())) {
      LOG.debug(String.format(virtualMachineMsg, vmSize, allowableVmSizes, region));
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
          localizationContext, null, virtualMachineMsg, vmSize, allowableVmSizes, region);
      isSuccessful = false;
    }

    return isSuccessful;
  }


  /**
   * Checks to see if Managed Disk option is on when user chooses to use custom image and validate
   * the image itself (existence & location).
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkUseCustomImage(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator, LocalizationContext localizationContext,
      Azure azure) {
    final String customImageOnlySupportsMdErrorMsg = "Custom image option is only supported when " +
        "using Managed Disks.";
    final String customImageGetByIdErrorMsg = "Failed to find custom image %s due to %s. The " +
        "image may not exist or the image resource ID is incorrect.";
    final String customImageDoesNotExistErrorMsg = "Custom image %s does not exist.";
    final String customImageInDifferentRegionErrorMsg = "Custom image %s is in a different " +
        "region (%s) from the environment (%s). The custom image must be in the same region " +
        "as the environment.";
    final boolean useManagedDisk = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        localizationContext).equals("Yes");
    final boolean useCustomImage = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
        localizationContext).equals("Yes");
    final String imageId = directorConfig.getConfigurationValue(
        ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
        localizationContext);

    if (!useCustomImage) {
      return;
    }

    if (!useManagedDisk) {
      LOG.debug(String.format(customImageOnlySupportsMdErrorMsg));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
          localizationContext, null, customImageOnlySupportsMdErrorMsg);
      return;
    }

    // make sure image exists and is in the correct region
    VirtualMachineCustomImage image;
    try {
      // getById throws exception if ID is malformed
      image = azure.virtualMachineCustomImages().getById(imageId);
    } catch (Exception e) {
      // AZURE_SDK FIXME Azure SDK throws NPE if the image does not exist
      LOG.debug(String.format(customImageGetByIdErrorMsg, imageId, e.getMessage()), e);
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
          localizationContext, null, customImageGetByIdErrorMsg, imageId, e.getMessage());
      return;
    }
    if (image == null) {
      LOG.debug(String.format(customImageDoesNotExistErrorMsg, imageId));
      addError(accumulator,
          ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
          localizationContext, null, customImageDoesNotExistErrorMsg, imageId);
      return;
    }
    if (!image.region().toString().equals(region)) {
      LOG.debug(String.format(customImageInDifferentRegionErrorMsg, imageId, image.region(),
          region));
      addError(accumulator,
          ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
          localizationContext, null, customImageInDifferentRegionErrorMsg, imageId,
          image.region(), region);
      return;
    }

    // check custom image purchase plan config
    try {
      Configurations.parseCustomImagePurchasePlanFromConfig(directorConfig, localizationContext);
    } catch (ValidationException e) {
      LOG.debug(e.getMessage());
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN,
          localizationContext, null, e.getMessage());
    }
  }

  void checkUserAssignedMsi(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    final String userAssignedMsiRg = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP,
        localizationContext);
    final String userAssignedMsiName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME,
        localizationContext);

    final String uaMsiNameMissingMsg = "User Assigned MSI Name is not specified, but User Assigned MSI Resource " +
        "Group is. Specify both to use User Assigned MSI or neither to not use MSI.";
    final String uaMsiRgMissingMsg = "User Assigned MSI Resource Group is not specified, but User Assigned MSI Name " +
        "is. Specify both to use User Assigned MSI or neither to not use MSI.";
    final String uaMsiRgDoesNotExistMsg = "User Assigned MSI Resource Group '" + userAssignedMsiRg + "' does not " +
        "exist. Create the Resource Group or use an existing one.";
    final String uaMsiDoesNotExistMsg = "User Assigned MSI '" + userAssignedMsiName + "' does not exist in Resource " +
        "Group '" + userAssignedMsiRg + "'. Create the User Assigned MSI or use an existing one.";

    // if no MSI fields are set then short-circuit
    if (StringUtils.isBlank(userAssignedMsiName) && StringUtils.isBlank(userAssignedMsiRg)) {
      // short-circuit return
      LOG.debug("Neither User Assigned MSI Name nor User Assigned MSI Resource Group are set, skip User Assigned MSI " +
          "validation.");
      return;
    }

    // at least one MSI field is set - error if only one is set
    if (StringUtils.isBlank(userAssignedMsiName)) {
      LOG.debug(uaMsiNameMissingMsg);
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME,
          localizationContext, null, uaMsiNameMissingMsg);
      return;
    } else if (StringUtils.isBlank(userAssignedMsiRg)) {
      LOG.debug(uaMsiRgMissingMsg);
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP,
          localizationContext, null, uaMsiRgMissingMsg);
      return;
    }

    // both MSI fields are set - validate them
    // validate that the RG exists
    if (!credentials.authenticate().resourceGroups().contain(userAssignedMsiRg)) {
      LOG.debug(uaMsiRgDoesNotExistMsg);
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP,
          localizationContext, null, uaMsiRgDoesNotExistMsg);
      return;
    }
    // validate that the MSI exists in the RG
    MSIManager msiManager = credentials.getMsiManager();
    Identity identity = msiManager.identities().getByResourceGroup(userAssignedMsiRg, userAssignedMsiName);
    if (identity == null) {
      LOG.debug(uaMsiDoesNotExistMsg);
      addError(accumulator, AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME,
          localizationContext, null, uaMsiDoesNotExistMsg);
    }
  }

  /**
   * Checks to see if the configured AAD group exists (in the same tenant as the service principal
   * used by the plugin. Also checks to see if the service principal can read AAD for group and
   * group member info.
   *
   * NOTE: This validator does not check for write privilege for adding member to AAD group.
   *
   * @param directorConfig Director config
   * @param accumulator error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkImplicitMsiGroupName(Configured directorConfig,
      PluginExceptionConditionAccumulator accumulator,
      LocalizationContext localizationContext) {
    final String aadGroupDoesNotExistMsg = "Failed to find AAD group '%s' in Tenant '%s'. Please " +
        "confirm the service principal has read/write access to the AAD tenant, the name of the " +
        "AAD group or create the group.";
    final boolean useImplicitMsi = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI,
        localizationContext).equals("Yes");

    if (!useImplicitMsi) {
      LOG.debug("Not using implicit MSI, skip AAD group name validation.");
      return;
    }

    final String aadGroupName = directorConfig.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME,
        localizationContext);

    if (aadGroupName == null || aadGroupName.isEmpty()) {
      LOG.debug("Skip implicit MSI AAD group name validation because it is not configured.");
      return;
    }

    GraphRbacManager graphRbacManager = credentials.getGraphRbacManager();
    ActiveDirectoryGroup aadGroup = graphRbacManager.groups().getByName(aadGroupName);
    if (aadGroup == null) {
      LOG.debug(String.format(aadGroupDoesNotExistMsg, aadGroupName, graphRbacManager.tenantId()));
      addError(accumulator,
          AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME,
          localizationContext, null, aadGroupDoesNotExistMsg, aadGroupName,
          graphRbacManager.tenantId());
      return;
    }
    LOG.info("AAD group '{}' exists in Tenant {}.", aadGroupName, graphRbacManager.tenantId());
  }
}
