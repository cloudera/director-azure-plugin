/*
 * Copyright (c) 2015 Cloudera, Inc.
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
 *  limitations under the License.
 *
 */

package com.cloudera.director.azure.compute.provider;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationValidator;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.Resource;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.SimpleInstanceState;
import com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.AvailabilitySet;
import com.microsoft.azure.management.compute.CachingTypes;
import com.microsoft.azure.management.compute.Disk;
import com.microsoft.azure.management.compute.DiskSkuTypes;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.compute.InstanceViewStatus;
import com.microsoft.azure.management.compute.PowerState;
import com.microsoft.azure.management.compute.PurchasePlan;
import com.microsoft.azure.management.compute.StorageAccountTypes;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineDataDisk;
import com.microsoft.azure.management.compute.VirtualMachineImage;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryGroup;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.microsoft.azure.management.network.PublicIPAddress;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.resources.fluentcore.arm.collection.SupportsDeletingByResourceGroup;
import com.microsoft.azure.management.resources.fluentcore.collection.SupportsDeletingById;
import com.microsoft.azure.management.resources.fluentcore.model.CreatedResources;
import com.microsoft.azure.management.storage.SkuName;
import com.microsoft.azure.management.storage.StorageAccount;
import com.microsoft.rest.ServiceCallback;
import com.microsoft.rest.ServiceFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

/**
 * Provider of compute resources (VMs) for Azure.
 */
public class AzureComputeProvider
    extends AbstractComputeProvider<AzureComputeInstance, AzureComputeInstanceTemplate> {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProvider.class);

  // The resource provider ID.
  public static final String ID = AzureComputeProvider.class.getCanonicalName();

  // The provider configuration properties.
  protected static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil
          .asConfigurationPropertyList(AzureComputeProviderConfigurationProperty.values());

  // The resource provider metadata.
  public static final ResourceProviderMetadata METADATA = SimpleResourceProviderMetadata
      .builder()
      .id(ID)
      .name("Azure")
      .description("Microsoft Azure compute provider")
      .providerClass(AzureComputeProvider.class)
      .providerConfigurationProperties(CONFIGURATION_PROPERTIES)
      .resourceTemplateConfigurationProperties(
          AzureComputeInstanceTemplate.getConfigurationProperties())
      .resourceDisplayProperties(AzureComputeInstance.getDisplayProperties())
      .build();

  private AzureCredentials credentials;

  private final ConfigurationValidator computeInstanceTemplateConfigValidator;

  private static final String MANAGED_OS_DISK_SUFFIX = "-OS";

  private static final int POLLING_INTERVAL_SECONDS = 5;

  // custom scheduler for async operations.
  private Scheduler scheduler = Schedulers.newThread();

  public AzureComputeProvider(Configured configuration, AzureCredentials credentials,
      LocalizationContext localizationContext) {
    super(configuration, METADATA, localizationContext);

    this.credentials = credentials;
    String region = this.getConfigurationValue(AzureComputeProviderConfigurationProperty.REGION,
        localizationContext);

    this.computeInstanceTemplateConfigValidator =
        new AzureComputeInstanceTemplateConfigurationValidator(credentials, region);
  }

  @Override
  public ConfigurationValidator getResourceTemplateConfigurationValidator() {
    if (!AzurePluginConfigHelper.validateResources()) {
      LOG.info("Skip all compute instance template configuration validator checks.");
      return super.getResourceTemplateConfigurationValidator();
    }
    return computeInstanceTemplateConfigValidator;
  }

  @Override
  public Resource.Type getResourceType() {
    return AbstractComputeInstance.TYPE;
  }

  @Override
  public AzureComputeInstanceTemplate createResourceTemplate(String name, Configured configuration,
      Map<String, String> tags) {
    return new AzureComputeInstanceTemplate(name, configuration, tags, getLocalizationContext());
  }

  /**
   * Helper function to delete VM and its resources. This function does best effort cleanup and
   * does not assume the VM or any of its resources are successfully created.
   *
   * @param azure the entry point object for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param vmName VM name (resource name: prefix + UUID)
   * @param commonResourceNamePrefix the name (or prefix) for the VM's resources
   * @param deletePublicIp boolean flag to indicate if we should delete the public IP
   * @return true if the VM and all its resources were successfully deleted; false otherwise
   */
  private boolean cleanupVmAndResourcesHelper(Azure azure, AzureComputeInstanceTemplate template,
      String vmName, String commonResourceNamePrefix, boolean deletePublicIp) {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    final String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    final boolean useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        templateLocalizationContext).equals("Yes");
    final int dataDiskCount = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT,
        templateLocalizationContext));

    StopWatch stopwatch = new StopWatch();
    LOG.info("Starting to delete VM {} and its resources.", vmName);
    stopwatch.start();

    // Deleting non-existent resource is a no-op by default, unless the SDK is configured to throw
    // exceptions when resources do not exist.
    boolean success = true;
    try {
      LOG.info("Deleting VM {}.", vmName);
      azure.virtualMachines().deleteByResourceGroup(computeRgName, vmName);
      LOG.info("Successful delete of VM {}.", vmName);
    } catch (Exception e) {
      LOG.error("Failed to delete VM: {}.", vmName, e);
      success = false;
    }

    if (useManagedDisks) {
      // populate the list of disks to delete async
      List<String> diskIds = new ArrayList<>();
      diskIds.add(commonResourceNamePrefix + MANAGED_OS_DISK_SUFFIX);
      for (int i = 0; i < dataDiskCount; i += 1) {
        diskIds.add(commonResourceNamePrefix + "-" + i);
      }

      // delete the disks async
      LOG.info("Start deleting {} Managed Disks.", diskIds);
      int successfulMdDeletes =
          asyncDeleteByResourceGroupHelper(azure.disks(), computeRgName, diskIds);
      LOG.info("Completed deleting {} ManagedDisks.", successfulMdDeletes);
    } else {
      try {
        LOG.info("Deleting Storage Account {}.", commonResourceNamePrefix);
        azure.storageAccounts().deleteByResourceGroup(computeRgName, commonResourceNamePrefix);
        LOG.info("Successful delete of Storage Account {}.", commonResourceNamePrefix);
      } catch (Exception e) {
        LOG.error("Failed to delete Storage Account {} for VM: {}.", commonResourceNamePrefix,
            vmName, e);
        success = false;
      }
    }

    try {
      LOG.info("Deleting NIC {}.", commonResourceNamePrefix);
      azure.networkInterfaces().deleteByResourceGroup(computeRgName, commonResourceNamePrefix);
      LOG.info("Successful delete of NIC {}.", commonResourceNamePrefix);
    } catch (Exception e) {
      LOG.error("Failed to delete Network Interface {} for VM: {}.", commonResourceNamePrefix,
          vmName, e);
      success = false;
    }
    if (deletePublicIp) {
      try {
        LOG.info("Deleting Public IP {}.", commonResourceNamePrefix);
        azure.publicIPAddresses().deleteByResourceGroup(computeRgName, commonResourceNamePrefix);
        LOG.info("Successful delete of Public IP {}.", commonResourceNamePrefix);
      } catch (Exception e) {
        LOG.error("Failed to delete Public IP {} for VM: {}.", commonResourceNamePrefix, vmName, e);
        success = false;
      }
    }

    stopwatch.stop();
    LOG.info("Delete of VM {} and its resources took {} seconds.", vmName,
        stopwatch.getTime() / 1000);
    return success;
  }

  /**
   * Atomically allocates multiple resources with the specified identifiers based on a single
   * resource template. If not all the resources can be allocated, the number of resources
   * allocated must at least the specified minimum or the method must fail cleanly with no
   * billing implications.
   *
   * Allocate is called individually per instance template. For example, once for each master,
   * worker, edge. xxx/all - how does batching fit in?
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds the ids of VMs to create
   * @param minCount the minimum number of VMs to create to consider the allocation successful
   * @throws InterruptedException
   */
  @Override
  public void allocate(final AzureComputeInstanceTemplate template, Collection<String> instanceIds,
      int minCount) throws InterruptedException {
    LOG.info("Preparing to allocate the following instances {}.", instanceIds);

    // get config
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    final String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    final String availabilitySetName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        templateLocalizationContext);
    final String vnrgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        templateLocalizationContext);
    final String vnName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
        templateLocalizationContext);
    final String nsgrgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        templateLocalizationContext);
    final String nsgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        templateLocalizationContext);
    final boolean createPublicIp = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        templateLocalizationContext).equals("Yes");
    final boolean useImplicitMsi = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI,
        templateLocalizationContext).equals("Yes");
    final String aadGroupName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME,
        templateLocalizationContext);

    // Include time for preparing VM create into total VM create time
    final StopWatch stopwatch = new StopWatch();
    stopwatch.start();

    boolean asSpecified;
    // custom log message depending on what we're searching for
    if (availabilitySetName == null || availabilitySetName.trim().isEmpty()) {
      asSpecified = false;
      // empty AS - log that we're not searching for it
      LOG.info("Searching for common Azure environment resources: Virtual Network {}, and Network " +
          "Security Group {} (no Availability Set specified - skipping).", vnName, nsgName);
    } else {
      asSpecified = true;
      LOG.info("Searching for common Azure environment resources: Virtual Network {}, Network " +
          "Security Group {}, and Availability Set {}.", vnName, nsgName, availabilitySetName);
    }
    Network vnet;
    NetworkSecurityGroup nsg;
    AvailabilitySet as = null;

    // Get common Azure resources for: VNET, NSG, and AS (if applicable).
    try {
      Azure azure = credentials.authenticate();

      // Invoke name() call to trigger an Azure backend call to make sure the common resources are still present.
      try {
        vnet = azure.networks().getByResourceGroup(vnrgName, vnName);
        vnet.name();
      } catch (Exception e) {
        String errorMessage = String.format("Unable to locate common Azure Virtual Network '%s' in Resource Group '%s'.",
            vnName, vnrgName);
        LOG.error(errorMessage);
        throw new UnrecoverableProviderException(errorMessage, e);
      }

      try {
        nsg = azure.networkSecurityGroups().getByResourceGroup(nsgrgName, nsgName);
        nsg.name();
      } catch (Exception e) {
        String errorMessage = String.format("Unable to locate common Azure Network Security Group '%s' in Resource " +
            "Group '%s'.", nsgName, nsgrgName);
        LOG.error(errorMessage);
        throw new UnrecoverableProviderException(errorMessage, e);
      }

      if (asSpecified) {
        try {
          as = azure.availabilitySets().getByResourceGroup(computeRgName, availabilitySetName);
          as.name();
        } catch (Exception e) {
          String errorMessage = String.format("Unable to locate common Azure Availability Set '%s' in Resource Group '%s'",
              vnName, computeRgName);
          LOG.error(errorMessage);
          throw new UnrecoverableProviderException(errorMessage, e);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Unable to authenticate with Azure.";
      LOG.error(errorMessage);
      throw new UnrecoverableProviderException(errorMessage, e);
    }
    LOG.info("Successfully found common Azure resources.");


    Set<ServiceFuture> vmCreates = new HashSet<>();
    final List<String> successfullyCreatedInstanceIds = Collections
        .synchronizedList(new ArrayList<String>());

    // Create VMs in parallel
    LOG.info("Starting to create the following instances {}.", instanceIds);
    for (final String instanceId : instanceIds) {
      final Azure azure = credentials.authenticate();
      final StopWatch perVmStopWatch = new StopWatch();
      perVmStopWatch.start();

      // Use the first 8 characters of the VM instance ID (UUID 4) to be the resource name.
      // Per RFC4122, the first 8 chars of UUID 4 are randomly generated.
      final String commonResourceNamePrefix = getFirstGroupOfUuid(instanceId);

      // build a VM creatable with all necessary resources attached it (storage, nic, public ip)
      final VirtualMachine.DefinitionStages.WithCreate vmCreatable;
      try {
        vmCreatable = buildVirtualMachineCreatable(azure, template, instanceId, as, vnet, nsg);
      } catch (Exception e) {
        LOG.error("Error while building VM Creatable with id {}: {}", instanceId, e.getMessage());

        // skip the rest
        continue;
      }

      final String vmName = vmCreatable.name();

      ServiceFuture future = azure.virtualMachines()
          .createAsync(
              new ServiceCallback<CreatedResources<VirtualMachine>>() {
                  @Override
                  public void failure(Throwable t) {
                    perVmStopWatch.stop();
                    LOG.error("Failed to create VM {} after {} seconds due to:", vmName,
                        stopwatch.getTime() / 1000, t);
                    cleanupVmAndResourcesHelper(azure, template, vmName, commonResourceNamePrefix,
                        createPublicIp);
                  }

                  @Override
                  public void success(CreatedResources<VirtualMachine> result) {
                    perVmStopWatch.stop();
                    LOG.info("Successfully created VM: {} in {} seconds.", vmName,
                        perVmStopWatch.getTime() / 1000);

                    // FIXME: delete the implicit MSI to group logic once explicit MSI is available
                    if (useImplicitMsi && aadGroupName != null && !aadGroupName.isEmpty()) {
                      GraphRbacManager graphRbacManager = credentials.getGraphRbacManager();
                      ActiveDirectoryGroup aadGroup = graphRbacManager.groups()
                          .getByName(aadGroupName);
                      if (aadGroup == null) {
                        LOG.error("AAD group '{}' does not exist in Tenant {}", aadGroupName,
                            graphRbacManager.tenantId());
                        return;
                      }
                      String msiObjId = result.get(vmCreatable.key())
                          .managedServiceIdentityPrincipalId();
                      try {
                        aadGroup.update().withMember(msiObjId).apply();
                      } catch (Exception e) {
                        LOG.error("Failed to add implicit MSI {} to AAD group {} for VM {} due to:",
                            msiObjId, aadGroupName, vmName, e);
                        return;
                      }
                      LOG.info("Successfully added implicit MSI {} to AAD group {} for VM {}.",
                          msiObjId, aadGroupName, vmName);
                    }

                    // Successfully created VM that failed MSI group join are not counted.
                    successfullyCreatedInstanceIds.add(instanceId);
                  }
                },
              vmCreatable);
      vmCreates.add(future);
    }

    // blocking poll
    while (vmCreates.size() > 0 &&
        (stopwatch.getTime() / 1000) <= AzurePluginConfigHelper.getAzureBackendOpPollingTimeOut()) {
      Set<ServiceFuture> completedCreates = new HashSet<>();
      for (ServiceFuture future : vmCreates) {
        if (future.isDone()) {
          completedCreates.add(future);
        }
      }
      vmCreates.removeAll(completedCreates);
      LOG.debug("Polling VM creates: {} completed, {} in progress, {} seconds have passed.",
          successfullyCreatedInstanceIds.size(), vmCreates.size(), stopwatch.getTime() / 1000);
      try {
        Thread.sleep(POLLING_INTERVAL_SECONDS * 1000);
      } catch (InterruptedException e) {
        // Handle interrupted create as a timeout event
        LOG.error("VM create is interrupted.");
        break;
      }
    }

    stopwatch.stop();

    // timeout handling
    if (vmCreates.size() > 0) {
      LOG.error("Creation for the following VMs have timed out after {} seconds: {}.",
          stopwatch.getTime() / 1000, getNewSubset(instanceIds, successfullyCreatedInstanceIds));
      for (ServiceFuture future : vmCreates) {
        // cancelling future does not trigger error handling
        future.cancel(true);
      }
    } else {
      LOG.info("Create Virtual Machines: it took {} seconds to create {} out of {} VMs with the " +
              "prefix {} from the group with instanceIds of: {}.",
          stopwatch.getTime() / 1000, successfullyCreatedInstanceIds.size(), instanceIds.size(),
          template.getInstanceNamePrefix(), instanceIds);
    }

    if (successfullyCreatedInstanceIds.size() < minCount) {
      LOG.info("Provisioned {} instances out of {}. minCount is {}.",
          successfullyCreatedInstanceIds.size(), instanceIds.size(), minCount);
      // Failed VMs and their resources are already cleaned up earlier or as failure callbacks.
      // Delete all the successfully created (including ones that failed the implicit MSI group
      // join), still in flight (timed out) VMs.
      // delete() is sufficient for cleaning up timed out VM because the VM create is almost
      // certainly in flight (and all it's dependent resources created) by the time create times
      // out. So we can rely on the creating VM to find and delete all its resources.
      delete(template, instanceIds);
      throw new UnrecoverableProviderException("Failed to create enough instances.");
    } else {
      LOG.info("Successfully provisioned {} instance(s) out of {}.",
          successfullyCreatedInstanceIds.size(), instanceIds.size());

      // Failed VMs and their resources are already cleaned up as failure callbacks earlier.
      // Delete all the VMs that failed the implicit MSI group join (they are successfully created),
      // and the VMs that are still in flight (timed out)
      delete(template, getNewSubset(instanceIds, successfullyCreatedInstanceIds));
    }
  }

  /**
   * Builds a new collection that is the original collection less items in the exclude collection.
   *
   * @return the new subset collection
   */
  private <T> Collection<T> getNewSubset(Collection<T> original, Collection<T> exclude) {
    Collection<T> newSet = new HashSet<>();
    newSet.addAll(original);
    newSet.removeAll(exclude);
    return newSet;
  }

  /**
   * Builds a PublicIPAddress Creatable to attach to the NIC.
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceId used in setting the domain
   * @param publicIpName what to name the Public IP resource
   * @return a Public IP Creatable to attach to a NIC
   */
  PublicIPAddress.DefinitionStages.WithCreate buildPublicIpCreatable(Azure azure,
      AzureComputeInstanceTemplate template, String instanceId, String publicIpName) {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    String location = this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    HashMap<String, String> tags =
        template.getTags().isEmpty() ? null : new HashMap<>(template.getTags());

    LOG.debug("PublicIPAddress Creatable {} building.", publicIpName);
    PublicIPAddress.DefinitionStages.WithCreate publicIpCreatable = azure
        .publicIPAddresses()
        .define(publicIpName)
        .withRegion(location)
        .withExistingResourceGroup(computeRgName)
        .withLeafDomainLabel(getDnsName(instanceId, template.getInstanceNamePrefix()));

    if (tags != null) {
      publicIpCreatable = publicIpCreatable.withTags(tags);
    }

    LOG.debug("PublicIPAddress Creatable {} built successfully.", publicIpName);
    return publicIpCreatable;
  }

  /**
   * Builds a NetworkInterface Creatable to attach to the VM.
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param commonResourceNamePrefix what to name the nic (and, if applicable, the Public IP)
   * @param vnet the virtual network to connect to
   * @param nsg the network security group to use
   * @param instanceId used in setting the Public IP domain
   * @return a Network Interface Creatable to attach to a VM
   */
  NetworkInterface.DefinitionStages.WithCreate buildNicCreatable(Azure azure,
      AzureComputeInstanceTemplate template, String commonResourceNamePrefix, Network vnet,
      NetworkSecurityGroup nsg, String instanceId) {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    String location = this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    String subnetName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME, templateLocalizationContext);
    final boolean createPublicIp = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        templateLocalizationContext).equals("Yes");
    HashMap<String, String> tags =
        template.getTags().isEmpty() ? null : new HashMap<>(template.getTags());

    LOG.debug("NetworkInterface Creatable {} building.", commonResourceNamePrefix);
    NetworkInterface.DefinitionStages.WithCreate nicCreatable = azure
        .networkInterfaces()
        .define(commonResourceNamePrefix)
        .withRegion(location)
        .withExistingResourceGroup(computeRgName)
        .withExistingPrimaryNetwork(vnet)
        .withSubnet(subnetName)
        // AZURE_SDK there is no way to set the IP allocation method to static
        .withPrimaryPrivateIPAddressDynamic()
        .withExistingNetworkSecurityGroup(nsg);

    if (createPublicIp) {
      // build and attach a public IP creatable
      nicCreatable = nicCreatable.withNewPrimaryPublicIPAddress(
          buildPublicIpCreatable(azure, template, instanceId, commonResourceNamePrefix));
    }

    if (tags != null) {
      nicCreatable = nicCreatable.withTags(tags);
    }

    LOG.debug("NetworkInterface Creatable {} built successfully.", commonResourceNamePrefix);
    return nicCreatable;
  }

  /**
   * Builds a Storage Account Creatable to attach to the VM
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param storageAccountName what to name the Storage Account
   * @return a Storage Account Creatable to attach to a VM
   */
  StorageAccount.DefinitionStages.WithCreate buildStorageAccountCreatable(Azure azure,
      AzureComputeInstanceTemplate template, String storageAccountName) {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    String location = this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    SkuName storageAccountType = SkuName.fromString(Configurations.convertStorageAccountTypeString(
        template.getConfigurationValue(
            AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
            templateLocalizationContext)));
    HashMap<String, String> tags =
        template.getTags().isEmpty() ? null : new HashMap<>(template.getTags());

    // IMPORTANT: Storage account names must have random prefixes to spread them into different
    // partitions (but not necessarily different storage stamps)
    LOG.debug("StorageAccount Creatable {} building.", storageAccountName);
    StorageAccount.DefinitionStages.WithCreate storageAccountCreatable = azure
        .storageAccounts()
        .define(storageAccountName)
        .withRegion(location)
        .withExistingResourceGroup(computeRgName)
        .withSku(storageAccountType);
    if (tags != null) {
      storageAccountCreatable = storageAccountCreatable.withTags(tags);
    }

    LOG.debug("StorageAccount Creatable {} built successfully.", storageAccountName);
    return storageAccountCreatable;
  }

  /**
   * Builds a Managed Disk Creatable to attach to the VM.
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param diskName what to name the Managed Disk
   * @return a Managed Disk Creatable to attach to a VM
   */
  Disk.DefinitionStages.WithCreate buildManagedDiskCreatable(Azure azure,
      AzureComputeInstanceTemplate template, String diskName) {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext());
    String location = this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    int dataDiskSizeGiB = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE,
        templateLocalizationContext));
    String storageType = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
        templateLocalizationContext);
    StorageAccountTypes storageAccountType = StorageAccountTypes.fromString(storageType);
    DiskSkuTypes diskSkuType = storageAccountType == StorageAccountTypes.STANDARD_LRS ?
        DiskSkuTypes.STANDARD_LRS : DiskSkuTypes.PREMIUM_LRS;
    HashMap<String, String> tags =
        template.getTags().isEmpty() ? null : new HashMap<>(template.getTags());

    LOG.debug("Disk Creatable {} building.", diskName);
    Disk.DefinitionStages.WithCreate mdCreatable = azure
        .disks()
        .define(diskName)
        .withRegion(location)
        .withExistingResourceGroup(computeRgName)
        .withData()
        .withSizeInGB(dataDiskSizeGiB)
        .withSku(diskSkuType);
    if (tags != null) {
      mdCreatable = mdCreatable.withTags(tags);
    }

    LOG.debug("Disk Creatable {} built successfully.", diskName);
    return mdCreatable;
  }

  /**
   * Builds the Virtual Machine Creatable and all other resources to attach to it (e.g. networking,
   * storage)
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceId used to construct the VM name
   * @param as the Availability Set to use
   * @param vnet the virtual network to connect to
   * @param nsg the network security group to use
   * @return the Virtual Machine Creatable used to build VMs
   */
  VirtualMachine.DefinitionStages.WithCreate buildVirtualMachineCreatable(Azure azure,
      AzureComputeInstanceTemplate template, String instanceId, AvailabilitySet as, Network vnet,
      NetworkSecurityGroup nsg) {
    LocalizationContext templateLocalizationContext = SimpleResourceTemplate
        .getTemplateLocalizationContext(getLocalizationContext());
    Region region = Region.findByLabelOrName(this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext));
    String imageString = template.getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.IMAGE,
        templateLocalizationContext);
    boolean useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        templateLocalizationContext).equals("Yes");
    String location = this.getConfigurationValue(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    String adminName = template.getConfigurationValue(
        ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME,
        templateLocalizationContext);
    String sshPublicKey = template.getConfigurationValue(
        ComputeInstanceTemplateConfigurationPropertyToken.SSH_OPENSSH_PUBLIC_KEY,
        templateLocalizationContext);
    int dataDiskCount = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT,
        templateLocalizationContext));
    int dataDiskSizeGiB = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE,
        templateLocalizationContext));
    String fqdnSuffix = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX,
        templateLocalizationContext);
    VirtualMachineSizeTypes vmSize = new VirtualMachineSizeTypes(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VMSIZE, templateLocalizationContext));
    HashMap<String, String> tags = template.getTags().isEmpty() ? null :
        new HashMap<>(template.getTags());
    String commonResourceNamePrefix = getFirstGroupOfUuid(instanceId);
    final boolean useCustomImage = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
        templateLocalizationContext).equals("Yes");
    final boolean useImplicitMsi = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI,
        templateLocalizationContext).equals("Yes");

    LOG.debug("VirtualMachine Creatable {} building.", instanceId);

    // get the image and plan
    ImageReference imageReference = null;
    PurchasePlan plan;
    if (useCustomImage) {
      plan = Configurations.parseCustomImagePurchasePlanFromConfig(template,
          templateLocalizationContext);
      if (plan != null) {
        LOG.info("Constructed purchase plan where publisher={}, product={}, name={} for custom " +
            "image.", plan.publisher(), plan.product(), plan.name());
      } else {
        LOG.info("No purchase plan configured for custom image.");
      }
    } else {
      try {
        imageReference = Configurations.parseImageFromConfig(template, templateLocalizationContext);
        String publisher = imageReference.publisher();
        String offer = imageReference.offer();
        String sku = imageReference.sku();
        String version = imageReference.version();

        // if it's a preview image construct the image and plan manually
        if (Configurations.isPreviewImage(imageReference)) {
          LOG.info("Image '{}' is a preview image with the fields publisher: {}; offer: {}; sku: {}; version: {}",
              imageString, publisher, offer, sku, version);
          plan = new PurchasePlan()
              .withName(sku)
              .withProduct(offer)
              .withPublisher(publisher);
        } else {
          VirtualMachineImage vmImage = azure.virtualMachineImages().getImage(region, publisher, offer, sku, version);

          imageReference = vmImage.imageReference();
          plan = vmImage.plan();
        }
      } catch (Exception e) {
        String errorMessage = String.format("Error while getting the VM Image and Plan: %s",
            e.getMessage());
        LOG.error(errorMessage);
        throw new UnrecoverableProviderException(errorMessage, e);
      }
    }

    // build a NIC creatable for this VM
    NetworkInterface.DefinitionStages.WithCreate nicCreatable = buildNicCreatable(azure, template,
        commonResourceNamePrefix, vnet, nsg, instanceId);

    VirtualMachine.DefinitionStages.WithCreate finalVmCreatable;

    if (useManagedDisks) {
      // translate the Storage Account Type String to Managed Disk specific types
      StorageAccountTypes storageAccountType = StorageAccountTypes.fromString(
          Configurations.convertStorageAccountTypeString(template.getConfigurationValue(
              AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE,
              templateLocalizationContext)));

      VirtualMachine.DefinitionStages.WithManagedCreate vmCreatable;
      VirtualMachine.DefinitionStages.WithOS vmCreatableBase = azure.virtualMachines()
          .define(getVmName(instanceId, template.getInstanceNamePrefix()))
          .withRegion(location)
          .withExistingResourceGroup(computeRgName)
          .withNewPrimaryNetworkInterface(nicCreatable);

      if (useCustomImage) {
        String imageId = template.getConfigurationValue(
            AzureComputeInstanceTemplateConfigurationProperty.IMAGE, templateLocalizationContext);
        vmCreatable = vmCreatableBase
            .withLinuxCustomImage(imageId)
            .withRootUsername(adminName)
            .withSsh(sshPublicKey)
            .withComputerName(getComputerName(instanceId, template.getInstanceNamePrefix(), fqdnSuffix));
      } else {
        vmCreatable = vmCreatableBase
            .withSpecificLinuxImageVersion(imageReference)
            .withRootUsername(adminName)
            .withSsh(sshPublicKey)
            .withComputerName(getComputerName(instanceId, template.getInstanceNamePrefix(), fqdnSuffix));
      }

      // make and attach the managed data disks
      for (int n = 0; n < dataDiskCount; n += 1) {
        vmCreatable = vmCreatable.withNewDataDisk(
            buildManagedDiskCreatable(azure, template, commonResourceNamePrefix + "-" + n));
      }

      finalVmCreatable = vmCreatable
          .withDataDiskDefaultStorageAccountType(storageAccountType)
          .withDataDiskDefaultCachingType(CachingTypes.NONE)
          .withOSDiskStorageAccountType(storageAccountType)
          .withOSDiskCaching(CachingTypes.READ_WRITE)
          .withOSDiskName(commonResourceNamePrefix + MANAGED_OS_DISK_SUFFIX)
          // .withOSDiskSizeInGB() // purposefully not set; defaults to 50GB
          .withSize(vmSize);
      if (as != null) {
        finalVmCreatable.withExistingAvailabilitySet(as);
      }
    } else {
      VirtualMachine.DefinitionStages.WithUnmanagedCreate vmCreatable = azure.virtualMachines()
          .define(getVmName(instanceId, template.getInstanceNamePrefix()))
          .withRegion(location)
          .withExistingResourceGroup(computeRgName)
          .withNewPrimaryNetworkInterface(nicCreatable)
          .withSpecificLinuxImageVersion(imageReference)
          .withRootUsername(adminName)
          .withSsh(sshPublicKey)
          .withUnmanagedDisks()
          .withComputerName(getComputerName(instanceId, template.getInstanceNamePrefix(), fqdnSuffix));

      // define the unmanaged data disks
      for (int n = 0; n < dataDiskCount; n++) {
        vmCreatable.defineUnmanagedDataDisk(null)
            .withNewVhd(dataDiskSizeGiB)
            .withCaching(CachingTypes.NONE)
            .attach();
      }
      finalVmCreatable = vmCreatable
          .withNewStorageAccount(
              buildStorageAccountCreatable(azure, template, commonResourceNamePrefix))
          .withOSDiskCaching(CachingTypes.READ_WRITE)
          .withSize(vmSize);
      if (as != null) {
        finalVmCreatable.withExistingAvailabilitySet(as);
      }
    }

    if (plan != null) {
      finalVmCreatable.withPlan(plan);
    }

    if (tags != null) {
      finalVmCreatable.withTags(tags);
    }

    if (useImplicitMsi) {
      finalVmCreatable.withManagedServiceIdentity();
    }

    LOG.debug("VirtualMachine Creatable {} built successfully.", instanceId);
    return finalVmCreatable;
  }

  /**
   * Returns current resource information for the specified instances, which are guaranteed to have
   * been created by this plugin. The VM will not be included in the return list if:
   * - the VM was not found (Azure backend returned null)
   * - there was an exception getting the VM's state (Azure backend call threw an exception)
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds the unique identifiers for the resources
   * @return the most currently available information, corresponding to the subset of the resources
   * which still exist. If an instance can't be found it's not included in the return list
   * @throws InterruptedException if the operation is interrupted
   */
  @Override
  public Collection<AzureComputeInstance> find(AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) throws InterruptedException {
    Collection<AzureComputeInstance> result = new ArrayList<>();
    List<String> vmNames = new ArrayList<>();
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext()));
    LOG.info("Finding in Resource Group {} with the instance prefix of {} the following VMs: {}.",
        rgName, template.getInstanceNamePrefix(), instanceIds);

    // AZURE_SDK authenticate doesn't actually reach out to Azure, wrapped in case that changes
    Azure azure;
    try {
      azure = credentials.authenticate();
    } catch (Exception e) {
      LOG.error("Error interacting with Azure: {} ", e.getMessage());
      return result;
    }

    for (String instanceId : instanceIds) {
      try {
        VirtualMachine virtualMachine = azure
            .virtualMachines()
            .getByResourceGroup(rgName, getVmName(instanceId, template.getInstanceNamePrefix()));

        if (virtualMachine == null) {
          LOG.debug("Virtual Machine {} with Instance ID {} in Resource Group {} not found",
              getVmName(instanceId, template.getInstanceNamePrefix()), instanceId, rgName);
          // no-op
        } else {
          LOG.debug("Virtual Machine {} with Instance ID {} in Resource Group {} found",
              getVmName(instanceId, template.getInstanceNamePrefix()), instanceId, rgName);
          // the instanceId field must be the instanceId Director is using (no prefix, UUID only)
          result.add(new AzureComputeInstance(template, instanceId, virtualMachine));
          vmNames.add(virtualMachine.name());
        }
      } catch (Exception e) { // catch-all
        LOG.error("Instance '{}' not found due to error: {}", instanceId, e.getMessage());
      }
    }

    LOG.info("Found the following VMs: {}.", vmNames);

    return result;
  }

  /**
   * Returns the virtual machine instance states. The status will be InstanceStatus.UNKNOWN if:
   * - the VM was not found (Azure backend returned null)
   * - there was an exception getting the VM's state (Azure backend call threw an exception)
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds list of IDs to get the state for
   * @return the VM states, or UNKNOWN if the state can't be found
   */
  @Override
  public Map<String, InstanceState> getInstanceState(AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) {
    HashMap<String, InstanceState> result = new HashMap<>();
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext()));
    LOG.info("Getting instance state in Resource Group {} with the instance prefix of {} the " +
        "following VMs: {}", rgName, template.getInstanceNamePrefix(), instanceIds);

    // authenticate doesn't actually reach out to Azure, wrapped in case that changes
    Azure azure;
    try {
      azure = credentials.authenticate();
    } catch (Exception e) {
      LOG.error("Error interacting with Azure: {} ", e.getMessage());
      // build the result set full of UNKNOWNs
      for (String instanceId : instanceIds) {
        result.put(instanceId, new SimpleInstanceState(InstanceStatus.UNKNOWN));
      }
      // short circuit
      return result;
    }

    for (String instanceId : instanceIds) {
      try {
        VirtualMachine virtualMachine = azure
            .virtualMachines()
            .getByResourceGroup(rgName, getVmName(instanceId, template.getInstanceNamePrefix()));

        if (virtualMachine == null) {
          LOG.debug("Virtual Machine {} with Instance ID {} in Resource Group {} not found",
              getVmName(instanceId, template.getInstanceNamePrefix()), instanceId, rgName);
          result.put(instanceId, new SimpleInstanceState(InstanceStatus.UNKNOWN));
        } else {
          LOG.debug("Virtual Machine {} with Instance ID {} in Resource Group {} found",
              getVmName(instanceId, template.getInstanceNamePrefix()), instanceId, rgName);
          result.put(instanceId,
              getVirtualMachineInstanceState(virtualMachine.instanceView().statuses()));
        }
      } catch (Exception e) { // catch-all
        LOG.error("Virtual Machine {} with Instance ID {} in Resource Group {} not found due to " +
            "error: {}", getVmName(instanceId, template.getInstanceNamePrefix()), instanceId, rgName, e.getMessage());
        result.put(instanceId, new SimpleInstanceState(InstanceStatus.UNKNOWN));
      }
    }

    return result;
  }

  /**
   * Helper function for deleting VM and relevant resources using Azure SDK's async interfaces.
   *
   * @param endpoint Azure endpoint for a particular type of resource
   * @param resourceName the human readable name of the endpoint
   * @param resourceIds list of resource IDs to be deleted
   * @param errorAccumulator accumulates error messages   @return true if all deletes were successful; false otherwise
   */
  boolean asyncDeleteByIdHelper(SupportsDeletingById endpoint, String resourceName, List<String> resourceIds,
      StringBuilder errorAccumulator) {
    HashSet<Completable> deleteCompletables = new HashSet<>();
    final List<String> successIds = Collections.synchronizedList(new ArrayList<String>());

    LOG.info("Start deleting these {} {}.", resourceIds.size(), resourceName);

    for (final String id : resourceIds) {
      // construct async operations
      Completable c = endpoint.deleteByIdAsync(id)
          .subscribeOn(scheduler)
          // log ID on error for manual cleanup
          .doOnError(new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
              LOG.error("Failed to delete: {}. Detailed reason: ", id, throwable);
            }
          })
          // count successful deletes
          .doOnCompleted(new Action0() {
            @Override
            public void call() {
              successIds.add(id);
              LOG.info("Successfully deleted: {}.", id);
            }
          })
          // log ID on timeout for manual cleanup
          .timeout(
              AzurePluginConfigHelper.getAzureBackendOpPollingTimeOut(), TimeUnit.SECONDS,
              Completable.fromAction(new Action0() {
                @Override
                public void call() {
                  // the id string contains full info (region, resource group name etc) for the
                  // resource. this is sufficient to identify the potentially orphaned resources.
                  LOG.error("Deletion of {} has timed out, check the corresponding Azure " +
                      "Resource Group for orphaned resources.", id);
                }
              }));
      deleteCompletables.add(c);
    }

    // block and wait on all async delete to complete or timeout
    try {
      Completable.mergeDelayError(deleteCompletables).await();
    } catch (RuntimeException e) {
      // FIXME handle InterruptedException
      LOG.error("Combined error for async delete:", e);
      // swallow exception, error already handled in the onError() callbacks
    }

    if (resourceIds.size() > successIds.size()) {
      // get the failed Ids
      resourceIds.removeAll(successIds);
      LOG.error("Deleted {} of {} {}. Failed to delete these: {}.",
          successIds.size(), resourceIds.size(), resourceName, Arrays.toString(resourceIds.toArray()));
      errorAccumulator.append(" ").append(resourceName).append("s: ").append(Arrays.toString(resourceIds.toArray()))
          .append(";");

      return false;
    } else {
      LOG.info("Successfully deleted all {}.", resourceName);

      return true;
    }
  }

  /**
   * Helper function for deleting resources using Azure SDK's async interfaces.
   *
   * @param endpoint Azure endpoint for a particular type of resource
   * @param rgName name of the Resource Group the resources are in
   * @param names names of the resources to delete
   * @return number of successful deletions
   */
  private int asyncDeleteByResourceGroupHelper(SupportsDeletingByResourceGroup endpoint,
      final String rgName, List<String> names) {
    HashSet<Completable> deleteCompletables = new HashSet<>();
    final AtomicInteger successCount = new AtomicInteger(0);

    LOG.info("Start deleting {} Azure resources.", names.size());

    for (final String name : names) {
      Completable c = endpoint.deleteByResourceGroupAsync(rgName, name)
          .subscribeOn(scheduler)
          // log name on error for manual cleanup
          .doOnError(new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
              LOG.error("Failed to delete: {}. Detailed reason: ", name, throwable);
            }
          })
          .doOnCompleted(new Action0() {
            @Override
            public void call() {
              successCount.incrementAndGet();
              LOG.info("Successfully deleted: {}.", name);
            }
          })
          // log name on timeout for manual cleanup
          .timeout(AzurePluginConfigHelper.getAzureBackendOpPollingTimeOut(), TimeUnit.SECONDS,
              Completable.fromAction(new Action0() {
            @Override
            public void call() {
              LOG.error("Deletion of {} has timed out, check the Resource Group {} for orphaned " +
                  "resources.", name, rgName);
            }
          }));
      deleteCompletables.add(c);
    }
    // block and wait on all async delete to complete or timeout
    try {
      Completable.mergeDelayError(deleteCompletables).await();
    } catch (RuntimeException e) {
      // FIXME handle InterruptedException
      LOG.error("Combined error for async delete:", e);
      // swallow exception, error already handled in the onError() callbacks
    }

    LOG.info("Successfully deleted {} of {} Azure resources.", successCount.get(), names.size());
    return successCount.get();
  }

  @Override
  public void delete(AzureComputeInstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException {
    if (instanceIds.isEmpty()) {
      // short circuit return (success)
      return;
    }

    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext()));
    boolean useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext()))
        .equals("Yes");
    boolean hasPublicIp = template.getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext())).equals("Yes");

    LOG.info("Preparing to delete VMs in Resource Group {}. Instance prefix: {}. VM Ids: {}.", rgName,
        template.getInstanceNamePrefix(), instanceIds);

    Collection<AzureComputeInstance> vmList = find(template, instanceIds);
    if (vmList.isEmpty()) {
      // short circuit return (success)
      return;
    }

    List<String> vmIds = new ArrayList<>();
    List<String> mdIds = new ArrayList<>();
    List<String> saIds = new ArrayList<>();
    List<String> nicIds = new ArrayList<>();
    List<String> pipIds = new ArrayList<>();

    // collect resources IDs for deletion
    LOG.info("Collect resource IDs for deletion.");
    Azure azure = credentials.authenticate();
    for (AzureComputeInstance vmToDelete : vmList) {
      final VirtualMachine vm = vmToDelete.getInstanceDetails();
      try {
        vmIds.add(vm.id());

        if (useManagedDisks) {
          mdIds.add(vmToDelete.getInstanceDetails().osDiskId());
          for (VirtualMachineDataDisk dataDisk : vmToDelete.getInstanceDetails().dataDisks()
              .values()) {
            mdIds.add(dataDisk.id());
          }
        } else {
          String saName = getStorageAccountNameFromVM(vmToDelete.getInstanceDetails());
          StorageAccount storageAccount = azure.storageAccounts()
              .getByResourceGroup(rgName, saName);
          saIds.add(storageAccount.id());
        }

        nicIds.add(vm.primaryNetworkInterfaceId());

        pipIds.add(vm.getPrimaryPublicIPAddressId());
      } catch (Exception e) {
        LOG.info("Error occurred while collecting resource IDs for deletion. " +
            "No resources will be deleted. Check Azure Resource Group {} for orphaned resources.",
            rgName, e);
        throw new UnrecoverableProviderException(e);
      }
    }

    LOG.info("Starting to delete VMs in Resource Group {}. Instance prefix: {}. VM Ids: {}.", rgName,
        template.getInstanceNamePrefix(), instanceIds);

    StopWatch stopwatch = new StopWatch();
    stopwatch.start();

    // Throw an UnrecoverableProviderException with a detailed error message if any of the deletes failed
    StringBuilder deleteFailedBuilder = new StringBuilder(
        String.format("Delete Failure - not all resources in Resource Group %s were deleted:", rgName));

    // Delete VMs
      boolean successfulVmDeletes = asyncDeleteByIdHelper(credentials.authenticate().virtualMachines(),
          "Virtual Machines", vmIds, deleteFailedBuilder);

    // Delete Storage
    boolean successfulStorageDeletes;
    if (useManagedDisks) {
      // Delete Managed Disks
      successfulStorageDeletes = asyncDeleteByIdHelper(credentials.authenticate().disks(), "Managed Disks", mdIds,
          deleteFailedBuilder);
    } else {
      // Delete Storage Accounts
      successfulStorageDeletes = asyncDeleteByIdHelper(credentials.authenticate().storageAccounts(), "Storage Accounts",
          saIds, deleteFailedBuilder);
    }

    // Delete NICs
    boolean successfulNicDeletes = asyncDeleteByIdHelper(credentials.authenticate().networkInterfaces(),
        "Network Interfaces", nicIds, deleteFailedBuilder);

    // Delete PublicIPs
    boolean successfulPipDeletes = true; // default to true
    if (hasPublicIp) {
      successfulPipDeletes = asyncDeleteByIdHelper(credentials.authenticate().publicIPAddresses(), "Public IPs", pipIds,
          deleteFailedBuilder);
    }

    stopwatch.stop();

    // Throw an UnrecoverableProviderException if any of the deletes failed
    if (!successfulVmDeletes || !successfulStorageDeletes || !successfulNicDeletes || !successfulPipDeletes) {
      // last bit of error string formatting
      deleteFailedBuilder.setLength(deleteFailedBuilder.length() - 1); // delete trailing ";"
      deleteFailedBuilder.append(".");

      throw new UnrecoverableProviderException(deleteFailedBuilder.toString());
    }

    LOG.info("Successfully deleted all {} of {} VMs + dependent resources in Resource Group {} in {} seconds. " +
        "Instance prefix: {}. VMs Ids: {}", instanceIds.size(), instanceIds.size(), rgName, stopwatch.getTime() / 1000,
        template.getInstanceNamePrefix(), instanceIds);
  }

  /**
   * Builds the VM name in this format:
   *
   *   [prefix]-[instance id]
   *   E.g. director-8c92779e-3241-47cc-8f5e-72d47fb109d5
   *
   * @param instanceId instance Id, provided by Director server
   * @param prefix instance name prefix
   * @return the VM name
   */
  static String getVmName(String instanceId, String prefix) {
    return prefix + "-" + instanceId;
  }

  /**
   * Builds the computer name in this format:
   *
   * - if there is no suffix:
   *   [prefix]-[first 8 of instance UUID]
   *   E.g. director-8c92779e
   * - if there is a suffix:
   *   [prefix]-[first 8 of instance UUID].[suffix]
   *   E.g. director-8c92779e.cdh-cluster.internal
   *
   * @param instanceId instance Id, provided by Director server
   * @param prefix instance name prefix
   * @param suffix fqdn suffix
   * @return the VM name
   */
  static String getComputerName(String instanceId, String prefix, String suffix) {
    if (suffix == null || suffix.trim().isEmpty()) {
      // no suffix
      return prefix + "-" + getFirstGroupOfUuid(instanceId);
    } else {
      // yes suffix
      return prefix + "-" + getFirstGroupOfUuid(instanceId) + "." + suffix;
    }
  }

  /**
   * Builds the domain name label in this format:
   *
   *   [prefix]-[first 8 of instance UUID]
   *   E.g. director-8c92779e
   *
   * @param instanceId instance Id, provided by Director server
   * @param prefix instance name prefix
   * @return the DNS name
   */
  static String getDnsName(String instanceId, String prefix) {
    return prefix + "-" + getFirstGroupOfUuid(instanceId);
  }

  /**
   * Returns the first group (first 8 characters) of an UUID string.
   *
   * Per RFC4122, the first 8 chars of UUID 4 are randomly generated.
   *
   * @param uuid UUID string
   * @return the first group (first 8 characters) of an UUID string
   */
  static String getFirstGroupOfUuid(String uuid) {
    return uuid.substring(0, 8);
  }

  /**
   * Extracts StorageAccount resource name from VM information.
   *
   * @param vm Azure VirtualMachine object, contains info about a particular VM
   * @return StorageAccount resource name
   */
  static String getStorageAccountNameFromVM(VirtualMachine vm) {
    String text = vm.storageProfile().osDisk().vhd().uri();
    String patternString = "^https://(.+?)\\..*";
    Pattern pattern = Pattern.compile(patternString);
    Matcher matcher = pattern.matcher(text);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Gets Director InstanceStates from Azure Statuses
   *
   * N.b. Azure has both PowerState and ProvisioningState, but there's no defined list or Enum for
   * ProvisioningState.
   *
   * xxx/all - this method is missing PowerStates and ProvisioningStates
   * xxx/all - 3/27: asked MSFT about PowerState and ProvisioningState
   * @param statuses
   * @return
   */
  private InstanceState getVirtualMachineInstanceState(List<InstanceViewStatus> statuses) {
    for (InstanceViewStatus status : statuses) {
      LOG.info("Status is {}.", status.code());
      if (status.code().equals(PowerState.RUNNING.toString())) {
        return new SimpleInstanceState(InstanceStatus.RUNNING);
      } else if (status.code().equals(PowerState.DEALLOCATED.toString())) {
        return new SimpleInstanceState(InstanceStatus.STOPPED);
      } else if (status.code().equals(PowerState.DEALLOCATING.toString())) {
        return new SimpleInstanceState(InstanceStatus.DELETING);
      } else if (status.code().equals(PowerState.UNKNOWN.toString())) {
        return new SimpleInstanceState(InstanceStatus.FAILED);
      } else if (status.code().toLowerCase().contains("fail")) {
        // AZURE_SDK Any state that has the word 'fail' in it indicates VM is in FAILED state
        return new SimpleInstanceState(InstanceStatus.FAILED);
      }
      LOG.info("Status {} did not match.", status.code());
    }
    return new SimpleInstanceState(InstanceStatus.UNKNOWN);
  }
}
