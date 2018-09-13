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
 *  limitations under the License.
 *
 */

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.compute.instance.VirtualMachine.create;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.MANAGED_OS_DISK_SUFFIX;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getBase64EncodedCustomData;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getComputerName;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getDnsName;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getVmName;
import static java.util.Objects.requireNonNull;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.microsoft.azure.CloudException;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.AvailabilitySet;
import com.microsoft.azure.management.compute.CachingTypes;
import com.microsoft.azure.management.compute.Disk;
import com.microsoft.azure.management.compute.DiskSkuTypes;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.compute.InstanceViewStatus;
import com.microsoft.azure.management.compute.PurchasePlan;
import com.microsoft.azure.management.compute.StorageAccountTypes;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineImage;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.graphrbac.ActiveDirectoryGroup;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.microsoft.azure.management.msi.Identity;
import com.microsoft.azure.management.msi.implementation.MSIManager;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.microsoft.azure.management.network.NicIPConfiguration;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Completable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

/**
 * On demand instance allocator.
 */
public class VirtualMachineAllocator implements InstanceAllocator {

  private static final int POLLING_INTERVAL_SECONDS = 5;
  private static final Logger LOG = LoggerFactory.getLogger(VirtualMachineAllocator.class);

  // custom scheduler for async operations.
  private final Scheduler scheduler = Schedulers.newThread();

  private final Azure azure;
  private final GraphRbacManager rbacManager;
  private final MSIManager msiManager;
  private final BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever;

  public VirtualMachineAllocator(
      Azure azure,
      GraphRbacManager rbacManager,
      MSIManager msiManager,
      BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever) {
    this.azure = requireNonNull(azure, "azure is null");
    this.rbacManager = requireNonNull(rbacManager, "rbacManager is null");
    this.msiManager = requireNonNull(msiManager, "msiManager is null");
    this.configRetriever = requireNonNull(configRetriever, "configRetriever is null");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<AzureComputeInstance<com.cloudera.director.azure.compute.instance.VirtualMachine>>
  allocate(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds,
      int minCount)
      throws InterruptedException {

    LOG.info("Preparing to allocate the following instances {}.", instanceIds);

    // get config
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
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
    String userAssignedMsiName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME,
        templateLocalizationContext);
    String userAssignedMsiRg = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP,
        templateLocalizationContext);
    final boolean useImplicitMsi = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI,
        templateLocalizationContext).equals("Yes");
    final String aadGroupName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME,
        templateLocalizationContext);
    final boolean withStaticPrivateIpAddress = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.WITH_STATIC_PRIVATE_IP_ADDRESS,
        templateLocalizationContext).equals("Yes");

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
    LOG.info("Successfully found common Azure resources.");


    Set<ServiceFuture> vmCreates = new HashSet<>();
    final List<String> successfullyCreatedInstanceIds = Collections
        .synchronizedList(new ArrayList<String>());
    final Collection<AzureComputeInstance<com.cloudera.director.azure.compute.instance.VirtualMachine>>
        successfullyCreatedInstances = Collections.synchronizedList(new ArrayList<>());

    // Create VMs in parallel
    LOG.info("Starting to create the following instances {}.", instanceIds);
    for (final String instanceId : instanceIds) {
      final StopWatch perVmStopWatch = new StopWatch();
      perVmStopWatch.start();

      // Use the first 8 characters of the VM instance ID (UUID 4) to be the resource name.
      // Per RFC4122, the first 8 chars of UUID 4 are randomly generated.
      final String commonResourceNamePrefix = getFirstGroupOfUuid(instanceId);

      // build a VM creatable with all necessary resources attached it (storage, nic, public ip)
      final VirtualMachine.DefinitionStages.WithCreate vmCreatable;
      try {
        vmCreatable = buildVirtualMachineCreatable(azure, localizationContext, template, instanceId, as, vnet, nsg);
      } catch (Exception e) {
        LOG.error("Error while building VM Creatable with id {}: ", instanceId, e);

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
                  cleanupVmAndResourcesHelper(azure, localizationContext, template, vmName, commonResourceNamePrefix,
                      createPublicIp);
                }

                @Override
                public void success(CreatedResources<VirtualMachine> result) {
                  // set the nic's primary private IP to static - this has to be done after the nic is created
                  if (withStaticPrivateIpAddress &&
                      !setDynamicPrivateIPStatic(result.get(vmCreatable.key()).getPrimaryNetworkInterface())) {
                    // short circuit return
                    return;
                  }

                  perVmStopWatch.stop();
                  LOG.info("Successfully created VM: {} in {} seconds.", vmName,
                      perVmStopWatch.getTime() / 1000);

                  // support both uaMSI and saMSI fields; if both are present only use uaMSI and skip the saMSI work
                  if ((StringUtils.isEmpty(userAssignedMsiName) && StringUtils.isEmpty(userAssignedMsiRg)) &&
                      (useImplicitMsi && !StringUtils.isEmpty(aadGroupName))) {
                    // FIXME: delete the implicit MSI to group logic once explicit MSI is available
                    ActiveDirectoryGroup aadGroup = rbacManager.groups()
                        .getByName(aadGroupName);
                    if (aadGroup == null) {
                      LOG.error("AAD group '{}' does not exist in Tenant {}", aadGroupName,
                          rbacManager.tenantId());
                      return;
                    }
                    String msiObjId = result.get(vmCreatable.key()).systemAssignedManagedServiceIdentityPrincipalId();
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
                  successfullyCreatedInstances
                      .add(new AzureComputeInstance<>(template, instanceId, create(result.get(vmCreatable.key()))));
                }
              },
              vmCreatable);
      vmCreates.add(future);
    }

    // blocking poll
    boolean interrupted = false;
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
        interrupted = true;
        LOG.error("VM create is interrupted.");
        break;
      }
    }

    stopwatch.stop();

    // timeout handling
    if (vmCreates.size() > 0) {
      LOG.error("Creation for the following VMs {} after {} seconds: {}.",
          interrupted ? "was interrupted" : "had timed out",
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
      LOG.error("Allocate failure: failed to create enough instances. {} instances out of {}. minCount is {}. " +
              "Successfully created instance ids: {}. Cleaning up the following instance ids: {}.",
          successfullyCreatedInstanceIds.size(), instanceIds.size(), minCount, successfullyCreatedInstanceIds,
          instanceIds);

      // Failed VMs and their resources are already cleaned up as failure callbacks earlier.
      // Delete all the successfully created VMs that failed the implicit MSI group join and the
      // VMs that are still in flight (timed out).

      delete(localizationContext, template, instanceIds);

      // failure
      LOG.info("Allocate failure: cleanup via delete() has succeeded.");
      throw new UnrecoverableProviderException("Failed to create enough instances.");
    } else {
      Collection<String> instanceIdsToDelete = getNewSubset(instanceIds, successfullyCreatedInstanceIds);
      if (instanceIdsToDelete.isEmpty()) {
        LOG.info("Allocate success: allocate successfully provisioned all {} instances. Successfully created " +
                "instance ids: {}.",
            successfullyCreatedInstanceIds.size(), successfullyCreatedInstanceIds);
      } else {
        LOG.info("Allocate success: successfully provisioned {} instances out of {} (min count is {}). Successfully " +
                "created instance ids: {}. Cleaning up the following failed instance ids: {}.",
            successfullyCreatedInstanceIds.size(), instanceIds.size(), minCount, successfullyCreatedInstanceIds,
            instanceIdsToDelete);

        // Failed VMs and their resources are already cleaned up as failure callbacks earlier.
        // Delete all the successfully created VMs that failed the implicit MSI group join and the
        // VMs that are still in flight (timed out).

        delete(localizationContext, template, instanceIdsToDelete);
        LOG.info("Allocate success: cleanup via delete() has succeeded.");
      }
      // success
      return successfullyCreatedInstances;
    }
  }

  /**
   * {@inheritDoc}
   *
   * If a VM has instance deleted, but not its associated resources, the instance will still
   * be included in the return list, but later calls to {@linkplain #getInstanceState}
   * should return failed for the instance.
   */
  @Override
  public Collection<AzureComputeInstance<com.cloudera.director.azure.compute.instance.VirtualMachine>>
  find(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException {
    Collection<AzureComputeInstance<com.cloudera.director.azure.compute.instance.VirtualMachine>> result =
        new ArrayList<>();
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext));
    String prefix = template.getInstanceNamePrefix();

    LOG.info("Finding in Resource Group {} with the instance prefix of {} the following VMs: {}.",
        rgName, template.getInstanceNamePrefix(), instanceIds);

    // all resource ids that were found (used for logging)
    List<AzureVirtualMachineMetadata> metadatas = new ArrayList<>();

    for (String instanceId : instanceIds) {
      AzureVirtualMachineMetadata metadata = new AzureVirtualMachineMetadata(azure, instanceId, template,
          localizationContext);

      if (metadata.resourcesExist()) {
        LOG.debug(metadata.toString());
        metadatas.add(metadata);

        // grab the vm object (which may or may not be null) to append to use in the result list
        VirtualMachine vm = azure.virtualMachines().getByResourceGroup(rgName, getVmName(instanceId, prefix));
        result.add(new AzureComputeInstance<>(template, instanceId, create(vm)));
      } else {
        LOG.debug(metadata.toString());
        // no-op
      }
    }

    LOG.info("All Virtual Machines in Resource Group {} to find are gathered. {}",
        rgName, AzureVirtualMachineMetadata.metadataListToString(metadatas));

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, List<InstanceViewStatus>> getInstanceState(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) {

    HashMap<String, List<InstanceViewStatus>> result = Maps.newHashMapWithExpectedSize(instanceIds.size());
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext));
    String prefix = template.getInstanceNamePrefix();

    LOG.info("Getting instance state in Resource Group {} with the instance prefix of {} the " +
        "following VMs: {}", rgName, template.getInstanceNamePrefix(), instanceIds);

    for (String instanceId : instanceIds) {
      try {
        VirtualMachine virtualMachine = azure
            .virtualMachines()
            .getByResourceGroup(rgName, getVmName(instanceId, prefix));

        // If the VM does not exist, but its associated resources exist, then assume that the VM is in a FAILED state.
        if (virtualMachine == null) {
          AzureVirtualMachineMetadata metadata = new AzureVirtualMachineMetadata(azure, instanceId, template,
              localizationContext);

          if (metadata.resourcesExist()) {
            // there are some resources which have failed to be deleted, mark the whole VM as failed

            LOG.debug("Virtual Machine {} in Resource Group {} was not found, but associated resources were found. " +
                    "Marking the VM state as FAILED. {}",
                instanceId, metadata.toString());
            result.put(instanceId, Collections.singletonList(new InstanceViewStatus().withCode("fail")));
          } else {
            LOG.debug("Virtual Machine {} in Resource Group {} was not found and no associated resources were found. " +
                    "Marking the VM state as UNKNOWN.",
                instanceId, rgName);
            result.put(instanceId, Collections.emptyList());
          }
        } else {
          List<InstanceViewStatus> status = virtualMachine.instanceView().statuses();
          LOG.debug("Virtual Machine {} in Resource Group {} found. Marking state as: {} ",
              instanceId, rgName, status.stream().map(InstanceViewStatus::code).toArray());
          result.put(instanceId, status);
        }
      } catch (Exception e) { // catch-all
        LOG.error("Virtual Machine {} in Resource Group {} not found due to error. Marking the VM state as UNKNOWN. " +
                "Error: {}",
            instanceId, rgName, e.getMessage());
        result.put(instanceId, Collections.emptyList());
      }
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException {

    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext));
    boolean useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext))
        .equals("Yes");
    boolean hasPublicIp = template.getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext)).equals("Yes");

    // short circuit return (success) if there aren't any instance ids to return
    if (instanceIds.isEmpty()) {
      LOG.info("No instance ids to delete: delete is successful.");
      return;
    }

    LOG.info("Gathering Azure Resource Ids to delete from Resource Group {} that are associated with instance ids: {}.",
        rgName, instanceIds);

    // collect the resource ids for deletion
    List<AzureVirtualMachineMetadata> metadatas = new ArrayList<>();
    try {
      for (String instanceId : instanceIds) {
        metadatas.add(new AzureVirtualMachineMetadata(azure, instanceId, template, localizationContext));
      }
    } catch (Exception e) {
      // throw an Unrecoverable if there's any problems getting resource ids
      LOG.error("Error occurred while collecting resource ids for deletion. No resources deleted and an " +
          "UnrecoverableProviderException will be thrown. Error: ", e);
      throw new UnrecoverableProviderException(e);
    }
    LOG.info("All Resource Ids in Resource Group {} to delete are gathered. {}",
        rgName, AzureVirtualMachineMetadata.metadataListToString(metadatas)
    );

    StopWatch stopwatch = new StopWatch();
    stopwatch.start();

    // Throw an UnrecoverableProviderException with a detailed error message if any of the deletes failed
    StringBuilder deleteFailedBuilder = new StringBuilder(
        String.format("Delete Failure - not all resources in Resource Group %s were deleted:", rgName));

    // Delete VMs
    boolean successfulVmDeletes = asyncDeleteByIdHelper(azure.virtualMachines(),
        "Virtual Machines", AzureVirtualMachineMetadata.getVmIds(metadatas), deleteFailedBuilder);

    // Delete Storage
    boolean successfulStorageDeletes;
    if (useManagedDisks) {
      // Delete Managed Disks
      successfulStorageDeletes = asyncDeleteByIdHelper(azure.disks(), "Managed Disks",
          AzureVirtualMachineMetadata.getMdIds(metadatas), deleteFailedBuilder);
    } else {
      // Delete Storage Accounts
      successfulStorageDeletes = asyncDeleteByIdHelper(azure.storageAccounts(), "Storage Accounts",
          AzureVirtualMachineMetadata.getSaIds(metadatas), deleteFailedBuilder);
    }

    // Delete NICs
    boolean successfulNicDeletes = asyncDeleteByIdHelper(azure.networkInterfaces(),
        "Network Interfaces", AzureVirtualMachineMetadata.getNicIds(metadatas), deleteFailedBuilder);

    // Delete PublicIPs
    boolean successfulPipDeletes = true; // default to true
    if (hasPublicIp) {
      successfulPipDeletes = asyncDeleteByIdHelper(azure.publicIPAddresses(), "Public IPs",
          AzureVirtualMachineMetadata.getpipIds(metadatas), deleteFailedBuilder);
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
            "Instance prefix: {}. VMs Ids: {}.", instanceIds.size(), instanceIds.size(), rgName, stopwatch.getTime() / 1000,
        template.getInstanceNamePrefix(), instanceIds);
  }

  /**
   * Builds a new collection that is the original collection less items in the
   * exclude collection.
   *
   * @return the new subset collection
   */
  private static <T> Collection<T> getNewSubset(Collection<T> original, Collection<T> exclude) {
    Collection<T> newSet = new HashSet<>(original);
    newSet.removeAll(exclude);
    return newSet;
  }

  /**
   * Builds a PublicIPAddress Creatable to attach to the NIC.
   *
   * @param azure        the entry point for accessing resource management
   *                     APIs in Azure
   * @param template     Azure compute instance template used to get user
   *                     provided fields
   * @param instanceId   used in setting the domain
   * @param publicIpName what to name the Public IP resource
   * @return a Public IP Creatable to attach to a NIC
   */
  @VisibleForTesting
  PublicIPAddress.DefinitionStages.WithCreate buildPublicIpCreatable(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String instanceId,
      String publicIpName) {

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    String location = configRetriever.apply(
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
   * @param azure                    the entry point for accessing resource
   *                                 management APIs in Azure
   * @param template                 Azure compute instance template used to
   *                                 get user provided fields
   * @param commonResourceNamePrefix what to name the nic (and, if applicable,
   *                                 the Public IP)
   * @param vnet                     the virtual network to connect to
   * @param nsg                      the network security group to use
   * @param instanceId               used in setting the Public IP domain
   * @return a Network Interface Creatable to attach to a VM
   */
  @VisibleForTesting
  NetworkInterface.DefinitionStages.WithCreate buildNicCreatable(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String commonResourceNamePrefix,
      Network vnet,
      NetworkSecurityGroup nsg,
      String instanceId) {

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    String location = configRetriever.apply(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    String subnetName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME, templateLocalizationContext);
    final boolean createPublicIp = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        templateLocalizationContext).equals("Yes");
    final boolean withAcceleratedNetworking = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.WITH_ACCELERATED_NETWORKING,
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

    if (withAcceleratedNetworking) {
      nicCreatable = nicCreatable.withAcceleratedNetworking();
    }

    if (createPublicIp) {
      // build and attach a public IP creatable
      nicCreatable = nicCreatable.withNewPrimaryPublicIPAddress(
          buildPublicIpCreatable(azure, localizationContext, template, instanceId, commonResourceNamePrefix));
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
   * @param azure              the entry point for accessing resource
   *                           management APIs in Azure
   * @param template           Azure compute instance template used to get
   *                           user provided fields
   * @param storageAccountName what to name the Storage Account
   * @return a Storage Account Creatable to attach to a VM
   */
  @VisibleForTesting
  StorageAccount.DefinitionStages.WithCreate buildStorageAccountCreatable(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String storageAccountName) {

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    String location = configRetriever.apply(
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
   * @param azure    the entry point for accessing resource management APIs in
   *                 Azure
   * @param template Azure compute instance template used to get user provided
   *                 fields
   * @param diskName what to name the Managed Disk
   * @return a Managed Disk Creatable to attach to a VM
   */
  @VisibleForTesting
  Disk.DefinitionStages.WithCreate buildManagedDiskCreatable(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String diskName) {

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    String location = configRetriever.apply(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    String computeRgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    int dataDiskSizeGiB = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE,
        templateLocalizationContext));
    DiskSkuTypes diskSkuType = DiskSkuTypes.fromStorageAccountType(StorageAccountTypes.fromString(
        Configurations.convertStorageAccountTypeString(template.getConfigurationValue(
            AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE, templateLocalizationContext))));
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
   * Builds the Virtual Machine Creatable and all other resources to attach to
   * it (e.g. networking, storage)
   *
   * @param azure      the entry point for accessing resource management APIs
   *                   in Azure
   * @param template   Azure compute instance template used to get user
   *                   provided fields
   * @param instanceId used to construct the VM name
   * @param as         the Availability Set to use
   * @param vnet       the virtual network to connect to
   * @param nsg        the network security group to use
   * @return the Virtual Machine Creatable used to build VMs
   */
  @VisibleForTesting
  VirtualMachine.DefinitionStages.WithCreate buildVirtualMachineCreatable(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String instanceId,
      AvailabilitySet as,
      Network vnet,
      NetworkSecurityGroup nsg) {

    LocalizationContext templateLocalizationContext = SimpleResourceTemplate
        .getTemplateLocalizationContext(localizationContext);
    Region region = Region.findByLabelOrName(configRetriever.apply(
        AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext));
    String imageString = template.getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.IMAGE,
        templateLocalizationContext);
    boolean useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        templateLocalizationContext).equals("Yes");
    String location = configRetriever.apply(
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
    VirtualMachineSizeTypes vmSize = VirtualMachineSizeTypes.fromString(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VMSIZE, templateLocalizationContext));
    HashMap<String, String> tags = template.getTags().isEmpty() ? null :
        new HashMap<>(template.getTags());
    String commonResourceNamePrefix = getFirstGroupOfUuid(instanceId);
    final boolean useCustomImage = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE,
        templateLocalizationContext).equals("Yes");
    String userAssignedMsiName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME,
        templateLocalizationContext);
    String userAssignedMsiRg = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP,
        templateLocalizationContext);
    final boolean useImplicitMsi = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI,
        templateLocalizationContext).equals("Yes");
    String customDataUnencoded = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_UNENCODED,
        templateLocalizationContext);
    String customDataEncoded = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_ENCODED,
        templateLocalizationContext);
    String base64EncodedCustomData = getBase64EncodedCustomData(customDataUnencoded, customDataEncoded);

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
    NetworkInterface.DefinitionStages.WithCreate nicCreatable = buildNicCreatable(azure, localizationContext, template,
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
            .withCustomData(base64EncodedCustomData)
            .withComputerName(getComputerName(instanceId, template.getInstanceNamePrefix(), fqdnSuffix));
      } else {
        vmCreatable = vmCreatableBase
            .withSpecificLinuxImageVersion(imageReference)
            .withRootUsername(adminName)
            .withSsh(sshPublicKey)
            .withCustomData(base64EncodedCustomData)
            .withComputerName(getComputerName(instanceId, template.getInstanceNamePrefix(), fqdnSuffix));
      }

      // make and attach the managed data disks
      for (int n = 0; n < dataDiskCount; n += 1) {
        vmCreatable = vmCreatable.withNewDataDisk(
            buildManagedDiskCreatable(azure, localizationContext, template, commonResourceNamePrefix + "-" + n));
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
          .withCustomData(base64EncodedCustomData)
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
              buildStorageAccountCreatable(azure, localizationContext, template, commonResourceNamePrefix))
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

    // support both uaMSI and saMSI; if both are present only use uaMSI
    if (!StringUtils.isEmpty(userAssignedMsiName) && !StringUtils.isEmpty(userAssignedMsiRg)) {
      Identity identity = msiManager.identities().getByResourceGroup(userAssignedMsiRg, userAssignedMsiName);
      finalVmCreatable.withExistingUserAssignedManagedServiceIdentity(identity);
    } else if (useImplicitMsi) {
      finalVmCreatable.withSystemAssignedManagedServiceIdentity();
    }

    LOG.debug("VirtualMachine Creatable {} built successfully.", instanceId);
    return finalVmCreatable;
  }

  /**
   * Helper function to delete VM and its resources. This function does best
   * effort cleanup and does not assume the VM or any of its resources are
   * successfully created.
   *
   * @param azure                    the entry point object for accessing
   *                                 resource management APIs in Azure
   * @param template                 Azure compute instance template used to
   *                                 get user provided fields
   * @param vmName                   VM name (resource name: prefix + UUID)
   * @param commonResourceNamePrefix the name (or prefix) for the VM's
   *                                 resources
   * @param deletePublicIp           boolean flag to indicate if we should
   *                                 delete the public IP
   * @return true if the VM and all its resources were successfully deleted;
   * false otherwise
   */
  private boolean cleanupVmAndResourcesHelper(
      Azure azure,
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      String vmName,
      String commonResourceNamePrefix,
      boolean deletePublicIp) {

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
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
   * Helper function for deleting VM and relevant resources using Azure SDK's
   * async interfaces.
   *
   * @param endpoint         Azure endpoint for a particular type of resource
   * @param resourceName     the human readable name of the endpoint
   * @param resourceIds      list of resource IDs to be deleted
   * @param errorAccumulator accumulates error messages   @return true if all
   *                         deletes were successful; false otherwise
   */
  @VisibleForTesting
  boolean asyncDeleteByIdHelper(
      SupportsDeletingById endpoint,
      String resourceName,
      List<String> resourceIds,
      StringBuilder errorAccumulator) {

    HashSet<Completable> deleteCompletables = new HashSet<>();
    final List<String> successIds = Collections.synchronizedList(new ArrayList<String>());

    LOG.info("Start deleting these {} {}: {}.", resourceIds.size(), resourceName, resourceIds);

    if (resourceIds.isEmpty()) {
      LOG.info("No {} to delete - delete is successful.", resourceName);

      // short-circuit return
      return true;
    }

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
      // cache the total number of resources
      int totalResources = resourceIds.size();
      // get the failed Ids
      resourceIds.removeAll(successIds);
      LOG.error("Deleted {} out of {} {}. Failed to delete these: {}.",
          successIds.size(), totalResources, resourceName, resourceIds);
      errorAccumulator.append(" ").append(resourceName).append("s: ").append(resourceIds).append(";");

      return false;
    } else {
      LOG.info("Successfully deleted all {}.", resourceName);

      return true;
    }
  }

  /**
   * Helper function for deleting resources using Azure SDK's async
   * interfaces.
   *
   * @param endpoint Azure endpoint for a particular type of resource
   * @param rgName   name of the Resource Group the resources are in
   * @param names    names of the resources to delete
   * @return number of successful deletions
   */
  private int asyncDeleteByResourceGroupHelper(
      SupportsDeletingByResourceGroup endpoint,
      final String rgName,
      List<String> names) {

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

  /**
   * Sets a NIC's primary private IP from dynamic to static.
   *
   * @param ni the NIC who's primary IP is being set
   * @return whether or not the private IP was successfully set to static
   */
  private boolean setDynamicPrivateIPStatic(NetworkInterface ni) {
    final AtomicInteger counter = new AtomicInteger(0);
    Retryer<Void> retryer = RetryerBuilder.<Void>newBuilder()
        .withWaitStrategy(WaitStrategies.exponentialWait(15, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(10))
        .retryIfExceptionOfType(CloudException.class)
        .build();

    try {
      retryer.call(() -> {
        LOG.debug("Network Interface {}: setting private IP to static attempt #{}.",
            ni.id(), counter.incrementAndGet());
        NicIPConfiguration ipConfig = ni.primaryIPConfiguration();
        ni.update()
            .updateIPConfiguration(ipConfig.name())
            .withPrivateIPAddressStatic(ipConfig.privateIPAddress())
            .parent()
            .apply();
        return null;
      });
    } catch (RetryException e) {
      LOG.error("Network Interface {}: failed to set private IP to static after {} attempts, marking " +
              "VM as failed.",
          ni.id(),
          e.getNumberOfFailedAttempts());

      // short-circuit return
      return false;
    } catch (Exception e) {
      LOG.error("Network Interface {}: failed to set private IP to static, marking VM as failed.",
          ni.id(), e);

      // short-circuit return
      return false;
    }
    LOG.info("Network Interface {}: set private IP to static successfully after {} attempt(s).",
        ni.id(),
        counter.get());

    return true;
  }
}
