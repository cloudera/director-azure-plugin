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

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationValidator;
import com.cloudera.director.azure.compute.instance.AzureInstance;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v2.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Resource;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleInstanceState;
import com.cloudera.director.spi.v2.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v2.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.InstanceViewStatus;
import com.microsoft.azure.management.compute.PowerState;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.microsoft.azure.management.msi.implementation.MSIManager;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider of compute resources (VMs) for Azure.
 */
public class AzureComputeProvider
    extends AbstractComputeProvider<AzureComputeInstance<? extends AzureInstance>, AzureComputeInstanceTemplate> {

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

  private final AzureCredentials credentials;
  private final ConfigurationValidator computeInstanceTemplateConfigValidator;

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
   * Atomically allocates multiple resources with the specified identifiers based on a single
   * resource template. If not all the resources can be allocated, the number of resources
   * allocated must be at least minCount or the method will make a good-faith effort to clean
   * up all resources so as to not leak anything, and then throw an exception.
   *
   * The breakdown of what happens and what is returned from this method for different scenarios:
   * - If allocate has successfully allocated all instances:
   *   This method will return a collection of all successfully allocated instances.
   * - If allocate has successfully allocated n instances, where minCount <= n < instanceIds.size():
   *   This method will attempt to clean up all failed resources and then it will return a
   *   collection of all successfully allocated instances. In this scenario it is possible that
   *   this method is unable to clean up all failed resources - Director, however, will attempt to
   *   clean up again by calling delete().
   * - If allocate has failed to allocate minCount instances:
   *   This method will attempt to clean up all resources - failed and successful - and then it
   *   will throw an exception containing information on why the VMs were unable to be created.
   *   In this scenario it is possible that this method is unable to clean up all resources -
   *   Director, however, will attempt to clean up again by calling delete().
   *
   * Note that Director does call delete to clean up the failed instances regardless. With an
   * initial cleanup attempt happening here at worst this will lead to a no-op when Director
   * attempts to delete the instances for a second time.
   *
   * Allocate MUST block until the Virtual Machine is successfully created or fails to be created.
   * This is required for Director (via the Azure Plugin) to distinguish between a VM that is
   * being created and one being deleted.
   *
   * This is because under the hood, to create an Azure VM, the plugin must first create Managed
   * Disks (or Storage Accounts), NICs, and Public IPs (optionally) before the VM itself (compute)
   * is created. That means that there will be a period of time where the VM itself does not exist
   * despite being in the process of being created. VM deletion is the reverse of the process where
   * the VM itself (compute) is deleted first followed by its associated resources (Storage, NIC,
   * Public IP). Consider the case where the VM itself (compute) is not present but its associated
   * resources (Storage, NIC, Public IP) are - it is impossible to distinguish between a VM being
   * created and one being deleted. Only when the VM (compute) is present can the plugin tell what
   * state (creating/deleting) the VM is in. As a result, during these phases, Director (via the
   * getInstanceState method) cannot correctly report VM state.
   *
   * To address this problem the allocate method blocks until the VM is successfully created or
   * fails to be created. This way Director will not attempt to poll for VM state until allocate
   * returns. As a result getInstanceState, which won't be called until after allocate returns, can
   * safely assume if the VM itself (compute) is missing, the VM must have failed.
   *
   * Allocate is called individually per instance template. For example, once for each master,
   * worker, edge. xxx/all - how does batching fit in?
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds the ids of VMs to create
   * @param minCount the minimum number of VMs to create to consider the allocation successful
   * @return a collection of the the successfully allocated resources. If minCount is satisfied,
   * it will return a collection of all successfully created resources - that collection will be
   * equal to or greater than minCount in size. Otherwise it will throw an exception. See details
   * in the Javadoc body of this method for different success/fail scenarios.
   * @throws InterruptedException
   */
  @Override
  public Collection<? extends AzureComputeInstance<? extends AzureInstance>> allocate(
      final AzureComputeInstanceTemplate template,
      Collection<String> instanceIds,
      int minCount)
      throws InterruptedException {
    return withInstanceAllocator(
        template.isAutomatic(),
        allocator -> allocator.allocate(getLocalizationContext(), template, instanceIds, minCount),
        InterruptedException.class);
  }



  /**
   * Returns current resource information for the specified instances, which are guaranteed to have been created by this
   * plugin. The VM will be included in the return list if it, or any of its associated resources (storage, networking,
   * public ip), still exist in Azure.
   *
   * The VM will not be included in the return list if:
   * - the VM and all its associated resources were not found (Azure backend returned null for all resources)
   * - there was an exception getting the VM's state (Azure backend call threw an exception)
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds the unique identifiers for the resources
   * @return the most currently available information, corresponding to the subset of the resources
   * which still exist. If an instance can't be found it's not included in the return list
   * @throws InterruptedException if the operation is interrupted
   */
  @Override
  public Collection<? extends AzureComputeInstance<? extends AzureInstance>> find(
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException {
    return withInstanceAllocator(
        template.isAutomatic(),
        allocator -> allocator.find(getLocalizationContext(), template, instanceIds),
        InterruptedException.class);
  }

  /**
   * Returns the virtual machine instance states.
   *
   * The status will be InstanceStatus.FAILED if the VM does not exist, but its associated resources exist. There's a
   * short window during allocate() where the VM create hasn't gotten far enough for the VM to show up in Azure
   * (as in, the Azure backend returns null). During this window (usually less than 60s) the VM state could be marked as
   * failed if getInstanceState() is called. In practice this does not matter because allocate() blocks until the VMs
   * have been created.
   *
   * The status will be InstanceStatus.UNKNOWN if:
   * - the VM was not found (Azure backend returned null)
   * - there was an exception getting the VM's state (Azure backend call threw an exception)
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceIds list of IDs to get the state for
   * @return the VM states, or UNKNOWN if the state can't be found
   */
  @Override
  public Map<String, InstanceState> getInstanceState(
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) {
    return withInstanceAllocator(
        template.isAutomatic(),
        allocator -> allocator.getInstanceState(getLocalizationContext(), template, instanceIds),
        RuntimeException.class)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> getVirtualMachineInstanceState(entry.getValue())));
  }

  @Override
  public void delete(AzureComputeInstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException {
    withInstanceAllocator(
        template.isAutomatic(),
        allocator -> {
          allocator.delete(getLocalizationContext(), template, instanceIds);
          return null;
        },
        InterruptedException.class);
  }

  /**
   * NOT IMPLEMENTED - empty Map is returned
   *
   * Returns a map from instance identifiers to a set of host key fingerprints for the specified
   * instances. The implementation can return an empty map to indicate that it cannot find the host
   * key fingerprints or that it does not support retrieving host key fingerprints. In that case
   * Director may use other means of host key fingerprint retrieval or may skip host key
   * verification. It may also return a partial map for the fingerprints that it managed to
   * find.
   *
   * @param template the resource template used to create the instance
   * @param instanceIds the unique identifiers for the instances
   * @return the map from instance identifiers to host key fingerprints for each instance
   * @throws InterruptedException if the operation is interrupted
   */
  @Override
  public Map<String, Set<String>> getHostKeyFingerprints(AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) throws InterruptedException {
    LOG.info("AzureComputeProvider.getHostKeyFingerprints() is NOT implemented.");
    return Maps.newHashMap();
  }

  /**
   * Gets Director InstanceStates from Azure Statuses.
   *
   * N.b. Azure has both PowerState and ProvisioningState, but there's no defined list or Enum for
   * ProvisioningState.
   *
   * xxx/all - this method is missing PowerStates and ProvisioningStates
   * xxx/all - 3/27: asked MSFT about PowerState and ProvisioningState
   * @param statuses a list of status information, from Azure
   * @return a Director's InstanceState derived from Azure's InstanceViewStatus
   */
  private static InstanceState getVirtualMachineInstanceState(List<InstanceViewStatus> statuses) {
    // used for printing out the list of statuses
    List<String> l = new ArrayList<>();
    for (InstanceViewStatus i : statuses) {
      l.add(i.code());
    }

    // check the statuses - see if any of them match
    LOG.debug("Checking these statuses: " + l);

    for (InstanceViewStatus status : statuses) {
      if (status.code().equals(PowerState.RUNNING.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.RUNNING);
        return new SimpleInstanceState(InstanceStatus.RUNNING);
      } else if (status.code().equals(PowerState.DEALLOCATING.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.STOPPING);
        return new SimpleInstanceState(InstanceStatus.STOPPING);
      } else if (status.code().equals(PowerState.DEALLOCATED.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.STOPPED);
        return new SimpleInstanceState(InstanceStatus.STOPPED);
      } else if (status.code().equals(PowerState.STARTING.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.PENDING);
        return new SimpleInstanceState(InstanceStatus.PENDING);
      } else if (status.code().equals(PowerState.STOPPED.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.STOPPED);
        return new SimpleInstanceState(InstanceStatus.STOPPED);
      } else if (status.code().equals(PowerState.STOPPING.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.STOPPING);
        return new SimpleInstanceState(InstanceStatus.STOPPING);
      } else if (status.code().equals(PowerState.UNKNOWN.toString())) {
        LOG.debug("Status is {}.", InstanceStatus.FAILED);
        return new SimpleInstanceState(InstanceStatus.FAILED);
      } else if (status.code().toLowerCase().contains("fail")) {
        LOG.debug("Status is {}.", InstanceStatus.FAILED);
        // AZURE_SDK Any state that has the word 'fail' in it indicates VM is in FAILED state
        return new SimpleInstanceState(InstanceStatus.FAILED);
      }
    }
    LOG.info("None of the VM statuses could be mapped to a SPI InstanceStatus. Statuses checked: " + l);
    return new SimpleInstanceState(InstanceStatus.UNKNOWN);
  }

  private <T, X extends Exception> T withInstanceAllocator(
      boolean isAutoScaling,
      FunctionX<InstanceAllocator, T, X> action,
      Class<X> exClass)
      throws X {

    try {
      Azure azure = credentials.authenticate();
      GraphRbacManager graphRbacManager = credentials.getGraphRbacManager();
      MSIManager msiManager = credentials.getMsiManager();
      InstanceAllocator instanceAllocator = createInstanceAllocator(
          isAutoScaling, azure, graphRbacManager, msiManager, this::getConfigurationValue);

      return action.apply(instanceAllocator);

    } catch (Exception e) {
      // Azure synchronous call uses rx java toBlocking on top of async call
      // Using reflection here to workaround checked exception X
      if (Thread.interrupted() || isCausedByInterrupt(e)) {
        if (exClass == InterruptedException.class) {
          try {
            throw exClass.newInstance();
          } catch (InstantiationException | IllegalAccessException ignore) {
          }
        } else {
          Thread.currentThread().interrupt();
          throw new UnrecoverableProviderException(e);
        }
      }

      Throwables.throwIfInstanceOf(e, exClass);
      Throwables.throwIfInstanceOf(e, UnrecoverableProviderException.class);
      Throwables.throwIfInstanceOf(e, RuntimeException.class);

      LOG.error("Failed in provider", e);
      throw new UnrecoverableProviderException(e);
    }
  }

  static boolean isCausedByInterrupt(Exception e) {
    return Throwables
        .getCausalChain(e)
        .stream()
        .anyMatch(ex -> InterruptedException.class.isInstance(e) || InterruptedIOException.class.isInstance(e));
  }

  @VisibleForTesting
  InstanceAllocator createInstanceAllocator(
      boolean isAutoScaling,
      Azure azure,
      GraphRbacManager graphRbacManager,
      MSIManager msiManager,
      BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever) {

    if (isAutoScaling) {
      return new VirtualMachineScaleSetAllocator(azure, msiManager, configRetriever);
    }
    return new VirtualMachineAllocator(azure, graphRbacManager, msiManager, configRetriever);
  }

  @FunctionalInterface
  private interface FunctionX<T, R, X extends Exception> {
    R apply(T input) throws X;
  }
}
