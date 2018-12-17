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

import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_ENCODED;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_UNENCODED;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.IMAGE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VMSIZE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.WITH_ACCELERATED_NETWORKING;
import static com.cloudera.director.azure.compute.instance.VirtualMachineScaleSetVM.create;
import static com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationProperty.REGION;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getBase64EncodedCustomData;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.VirtualMachineAllocator.GET_HOST_KEY_FINGERPRINT;
import static com.cloudera.director.azure.utils.AzurePluginConfigHelper.getVMSSOpTimeout;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_OPENSSH_PUBLIC_KEY;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.microsoft.azure.management.compute.VirtualMachineScaleSetSkuTypes.fromSkuNameAndTier;

import static java.util.Objects.requireNonNull;

import static org.apache.commons.lang3.StringUtils.substring;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.VirtualMachineScaleSetVM;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.microsoft.azure.SubResource;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.CachingTypes;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.compute.InstanceViewStatus;
import com.microsoft.azure.management.compute.Plan;
import com.microsoft.azure.management.compute.PowerState;
import com.microsoft.azure.management.compute.PurchasePlan;
import com.microsoft.azure.management.compute.RunCommandResult;
import com.microsoft.azure.management.compute.StorageAccountTypes;
import com.microsoft.azure.management.compute.VirtualMachineImages;
import com.microsoft.azure.management.compute.VirtualMachineInstanceView;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet.DefinitionStages.WithCreate;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet.DefinitionStages.WithLinuxCreateManaged;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet.DefinitionStages.WithLinuxCreateManagedOrUnmanaged;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet.DefinitionStages.WithManagedCreate;
import com.microsoft.azure.management.compute.VirtualMachineScaleSet.DefinitionStages.WithOS;
import com.microsoft.azure.management.compute.VirtualMachineScaleSetNetworkConfiguration;
import com.microsoft.azure.management.compute.VirtualMachineScaleSetPublicIPAddressConfiguration;
import com.microsoft.azure.management.compute.VirtualMachineScaleSets;
import com.microsoft.azure.management.compute.implementation.VirtualMachineScaleSetImpl;
import com.microsoft.azure.management.compute.implementation.VirtualMachineScaleSetInner;
import com.microsoft.azure.management.msi.Identities;
import com.microsoft.azure.management.msi.Identity;
import com.microsoft.azure.management.msi.implementation.MSIManager;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.network.Networks;
import com.microsoft.azure.management.network.implementation.PublicIPAddressInner;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

/**
 * A class to allocate virtual machine scale set in Azure.
 */
class VirtualMachineScaleSetAllocator implements InstanceAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(VirtualMachineScaleSetAllocator.class);
  private static final String STANDARD_TIER = "Standard";
  private static final String TRUE = "Yes";
  private static final int PUBLIC_IP_IDLE_TIMEOUT_IN_MIN = 15;
  private static final Pattern PUBLIC_IP_ID_PATTERN = Pattern.compile(
      ".+/providers/Microsoft.Compute/virtualMachineScaleSets/.+/virtualMachines/(.+)/networkInterfaces/.+");

  private final Azure azure;
  private final MSIManager msiManager;
  private final BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever;

  /**
   * Creates an instance of VirtualMachineScaleSetAllocator.
   *
   * @param azure           azure client
   * @param msiManager      MSI manager
   * @param configRetriever a function to retrieve config
   */
  VirtualMachineScaleSetAllocator(
      Azure azure,
      MSIManager msiManager,
      BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever) {
    this.azure = requireNonNull(azure, "azure is null");
    this.msiManager = requireNonNull(msiManager, "msiManager is null");
    this.configRetriever = requireNonNull(configRetriever, "configRetriever is null");
  }

  /**
   * {@inheritDoc}
   */
  /*
   * Note: we don't clean up as long as minCount is satisfied, because grow calls the same function, that
   * we don't want to clean up the existing instances if grow fails.
   * There are two ways to solve the problem:
   * 1. unbounded wait (user can override the vmss timeout setting), but it needs human interation if bad
   * thing happens
   * 2. potential temporarily leaked instances, which will be handled by autorepair if enabled. User can
   * also add the instance(s) back to cluster by calling UpdateCluster
   */
  @Override
  public Collection<AzureComputeInstance<VirtualMachineScaleSetVM>> allocate(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds,
      int minCount)
      throws InterruptedException {

    requireNonNull(localizationContext, "localizationContext is null");
    requireNonNull(template, "template is null");
    requireNonNull(instanceIds, "instanceIds is null");
    checkArgument(minCount >= 0, "minCount is negative");

    LocalizationContext context = SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    boolean useCustomImage = template.getConfigurationValue(USE_CUSTOM_MANAGED_IMAGE, context).equalsIgnoreCase(TRUE);

    WithCreate creatableVmss =
        new WithBasic(template, configRetriever, context, azure.virtualMachineScaleSets(), azure.networks())
            .andThen(useCustomImage ?
                new WithCustomImage(template, context)
                    .andThen(new WithManagedDisks<>(template, context)) :
                new WithNonCustomImage(template, context, azure.virtualMachineImages())
                    .andThen(new WithManagedDisks<>(template, context)))
            .andThen(new WithOtherConfigs(template, context, msiManager.identities(), azure.subscriptionId(), instanceIds.size()))
            .apply(null);

    Throwable[] exception = new Throwable[1];

    List<AzureComputeInstance<VirtualMachineScaleSetVM>> instances = timed(
        () -> Observable.merge(
            creatableVmss
                .createAsync() // createAsync is createOrUpdate
                .onErrorResumeNext(e -> {
                  LOG.warn("Error createOrUpdate " + creatableVmss.name(), e);
                  exception[0] = e;
                  return getVirtualMachineScaleSet(template, context);
                }),
            Observable
                .timer(getVMSSOpTimeout(), TimeUnit.SECONDS)
                .flatMap(i -> {
                  LOG.warn("CreateOrUpdate timeout for " + creatableVmss.name());
                  return getVirtualMachineScaleSet(template, context);
                }))
            .map(vmss -> convert(template, context, (VirtualMachineScaleSet) vmss, Collections.emptySet()))
            .toBlocking() // no cleanup if interrupted because it can happen during update
            .first(),
        "Creating vmss " + creatableVmss.name());

    if (instances.size() >= minCount) {
      // because of overprovisioning, we might have > expectedCount instances in vmss
      if (instances.size() > instanceIds.size()) {
        instances = sortAndTerminateLatest(instances, instanceIds.size());
      }

      return instances.stream().limit(instanceIds.size()).collect(Collectors.toList());
    }

    String errorMsg = String.format(
        "Virtual machine scale set %s failed to reach min count %d before timeout.",
        creatableVmss.name(),
        minCount);
    Throwable ex = exception[0] == null ? new RuntimeException(errorMsg) : exception[0];
    LOG.warn(errorMsg, ex);

    delete(localizationContext, template, ex);
    throw new UnrecoverableProviderException(ex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<AzureComputeInstance<VirtualMachineScaleSetVM>> find(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException {

    requireNonNull(localizationContext, "localizationContext is null");
    requireNonNull(template, "template is null");
    requireNonNull(instanceIds, "instanceIds is null");

    LocalizationContext context = SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    VirtualMachineScaleSet vmss = getVirtualMachineScaleSet(template, context).toBlocking().first();

    return convert(template, context, vmss, Sets.newHashSet(instanceIds));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, List<InstanceViewStatus>> getInstanceState(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) {

    requireNonNull(localizationContext, "localizationContext is null");
    requireNonNull(template, "template is null");
    requireNonNull(instanceIds, "instanceIds is null");

    LocalizationContext context = SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);
    VirtualMachineScaleSet vmss = getVirtualMachineScaleSet(template, context).toBlocking().first();

    if (vmss == null) {
      return Maps.toMap(instanceIds, id -> Collections.emptyList());
    }

    Map<String, List<InstanceViewStatus>> found = vmss
        .virtualMachines()
        .list()
        .stream()
        .filter(vm -> instanceIds.isEmpty() || instanceIds.contains(vm.name()))
        .collect(Collectors.toMap(
            com.microsoft.azure.management.compute.VirtualMachineScaleSetVM::name,
            vm -> {
              VirtualMachineInstanceView instanceView = vm.instanceView();
              return instanceView != null ? instanceView.statuses() : Collections.<InstanceViewStatus>emptyList();
            }));

    return Maps.toMap(instanceIds, id -> found.containsKey(id) ? found.get(id) : Collections.emptyList());
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

    requireNonNull(localizationContext, "localizationContext is null");
    requireNonNull(template, "template is null");

    LocalizationContext context = SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext);

    if (instanceIds.isEmpty()) {
      timed(
          () -> {
            azure
                .virtualMachineScaleSets()
                .deleteById(getId(
                    azure.subscriptionId(),
                    template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, context),
                    ResourceProvider.VMSS,
                    getVirtualMachineScaleSetName(template.getInstanceNamePrefix(), template.getGroupId())));
            return null;
          },
          "Deleting vmss " + template.getGroupId());

    } else {
      // TODO: shrink
      // 1. get whether scaling is enabled
      // 2. disable scaling

      timed(
          () -> {
            azure
                .virtualMachineScaleSets()
                .getById(getId(
                    azure.subscriptionId(),
                    template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, context),
                    ResourceProvider.VMSS,
                    getVirtualMachineScaleSetName(template.getInstanceNamePrefix(), template.getGroupId())))
                .virtualMachines()
                .deleteInstances(
                    instanceIds
                        .stream()
                        .map(VirtualMachineScaleSetAllocator::extractInstanceId)
                        .toArray(String[]::new));
            return null;
          },
          "Deleting vmss instances " + instanceIds);

      // 4. adjust min count and restore scaling if needed
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Set<String>> getHostKeyFingerprints(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds) throws InterruptedException {

    Map<String, Set<String>> instanceIdsToHostKeyFingerprints = Maps.newHashMap();
    Map<String, Observable<RunCommandResult>> hostKeyObservables = Maps.newHashMap();
    String resourceGroupName = template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, localizationContext);

    for (String instanceId : instanceIds) {
      String extractedInstanceId = extractInstanceId(instanceId);
      Observable<RunCommandResult> result = azure
          .virtualMachineScaleSets()
          .runCommandVMInstanceAsync(
              resourceGroupName,
              getVirtualMachineScaleSetName(template.getInstanceNamePrefix(), template.getGroupId()),
              extractedInstanceId,
              GET_HOST_KEY_FINGERPRINT);
      hostKeyObservables.put(extractedInstanceId, result);
    }

    for (Map.Entry<String, Observable<RunCommandResult>> keyAndResult : hostKeyObservables.entrySet()) {
      RunCommandResult runCommandResult = keyAndResult.getValue().toBlocking().first();
      Set<String> fingerprints = AzureVirtualMachineMetadata
          .getHostKeysFromCommandOutput(runCommandResult.value().get(0).message());
      instanceIdsToHostKeyFingerprints.put(keyAndResult.getKey(), fingerprints);
    }

    return instanceIdsToHostKeyFingerprints;
  }

  private List<AzureComputeInstance<VirtualMachineScaleSetVM>> convert(
      AzureComputeInstanceTemplate template,
      LocalizationContext context,
      VirtualMachineScaleSet vmss,
      Set<String> instanceIds) {
    if (vmss == null) {
      return Collections.emptyList();
    }

    requireNonNull(template, "template is null");
    requireNonNull(instanceIds, "instanceIds is null");

    // TODO: if we find the API call to be slow we should try to use javarx join or groupjoin
    Map<String, PublicIPAddressInner> instanceIdToPublicIp =
        !template.getConfigurationValue(PUBLIC_IP, context).equalsIgnoreCase(TRUE) ?
            Collections.emptyMap() :
            azure
                .publicIPAddresses()
                .inner()
                .listVirtualMachineScaleSetPublicIPAddresses(vmss.resourceGroupName(), vmss.name())
                .stream()
                .collect(Collectors.toMap(ip -> getInstanceId(ip.id()), Function.identity()));

    return vmss
        .virtualMachines()
        .list()
        .stream()
        .filter(vm -> instanceIds.isEmpty() || instanceIds.contains(vm.name()))
        .map(vm -> new AzureComputeInstance<>(
            template,
            vm.name(),
            create(vm, instanceIdToPublicIp.get(vm.instanceId()))))
        .collect(Collectors.toList());
  }

  private static String getId(
      String subscriptionId,
      String resourceGroupId,
      ResourceProvider resourceProvider,
      String groupId) {
    return String.format(
        "/subscriptions/%s/resourceGroups/%s/providers/%s/%s",
        subscriptionId,
        resourceGroupId,
        resourceProvider.getName(),
        groupId);
  }


  private static String extractInstanceId(String instanceName) {
    // instance name is ${user_provided_name}_${instance_id}, whereas ${instance_id} is numeric
    return instanceName.substring(instanceName.lastIndexOf('_') + 1);
  }

  @VisibleForTesting
  static String getInstanceId(String publicIpId) {
    requireNonNull(publicIpId, "publicIpId is null");

    // see utest for id pattern
    Matcher matcher = PUBLIC_IP_ID_PATTERN.matcher(publicIpId);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    throw new IllegalStateException("Cannot parse public IP ID: " + publicIpId);
  }

  @VisibleForTesting
  static String getVirtualMachineScaleSetName(String instanceNamePrefix, String groupId) {
    checkArgument(StringUtils.isNotBlank(instanceNamePrefix));
    checkArgument(StringUtils.isNotBlank(groupId));
    return String.format("%s-%s", instanceNamePrefix, groupId);
  }

  // https://docs.microsoft.com/en-us/azure/templates/microsoft.compute/virtualmachinescalesets
  @VisibleForTesting
  static String getComputerNamePrefix(String instanceNamePrefix, String groupId) {
    checkArgument(StringUtils.isNotBlank(instanceNamePrefix));
    checkArgument(StringUtils.isNotBlank(groupId));
    return String.format("%s-%s", substring(instanceNamePrefix, 0, 6), getFirstGroupOfUuid(groupId));
  }

  private List<AzureComputeInstance<VirtualMachineScaleSetVM>> sortAndTerminateLatest(
      List<AzureComputeInstance<VirtualMachineScaleSetVM>> instances,
      int limit) {
    List<AzureComputeInstance<VirtualMachineScaleSetVM>> result = instances
        .stream()
        .sorted(VirtualMachineStateComparator.INSTANCE)
        .collect(Collectors.toList());

    try {
      instances.stream().skip(limit).forEach(instance -> {
        LOG.info("Terminating instance " + instance.unwrap().name());
        // best effort to hint Azure which overprovisioned instance to terminate, but not guaranteed
        instance.unwrap().deleteAsync();
      });
    } catch (Exception e) {
      LOG.warn("Instance leak ", e);
    }
    return result;
  }

  private void delete(
      LocalizationContext localizationContext, AzureComputeInstanceTemplate template, Throwable ex) {
    boolean interrupted = Thread.interrupted();

    try {
      delete(localizationContext, template, Collections.emptyList());
    } catch (Exception inner) {
      ex.addSuppressed(inner);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private Observable<VirtualMachineScaleSet> getVirtualMachineScaleSet(
      AzureComputeInstanceTemplate template, LocalizationContext context) {
    return azure
        .virtualMachineScaleSets()
        .getByIdAsync(getId(
            azure.subscriptionId(),
            template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, context),
            ResourceProvider.VMSS,
            getVirtualMachineScaleSetName(template.getInstanceNamePrefix(), template.getGroupId())));
  }

  private static <T> T timed(Supplier<T> action, String message) {
    requireNonNull(action, "action is null");
    if (message != null) {
      LOG.info("{} started", message);
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      return action.get();
    } catch (Exception e) {
      if (message != null) {
        LOG.warn("{} failed", message, e);
      }
      throw e;
    } finally {
      stopwatch.stop();
      LOG.info(
          "{} finished in {} msec",
          message != null ? message : "Action",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @VisibleForTesting
  static class VirtualMachineStateComparator implements Comparator<AzureComputeInstance<VirtualMachineScaleSetVM>> {
    private static final VirtualMachineStateComparator INSTANCE = new VirtualMachineStateComparator();

    @VisibleForTesting
    static final Map<PowerState, Integer> POWER_STATES = ImmutableMap
        .<PowerState, Integer>builder()
        .put(PowerState.RUNNING, 1)
        .put(PowerState.STARTING, 2)
        .put(PowerState.DEALLOCATING, 3)
        .put(PowerState.DEALLOCATED, 4)
        .put(PowerState.UNKNOWN, 5)
        .put(PowerState.STOPPING, 6)
        .put(PowerState.STOPPED, 7)
        .build();

    // Note: this comparator imposes orderings that are inconsistent with equals.
    @Override
    public int compare(
        AzureComputeInstance<VirtualMachineScaleSetVM> lhs,
        AzureComputeInstance<VirtualMachineScaleSetVM> rhs) {
      // Note: if Azure introduces more powerstate not represented here, it will be mapped to PowerState.UNKNOWN
      return POWER_STATES.get(MoreObjects.firstNonNull(lhs.unwrap().powerState(), PowerState.UNKNOWN))
          - POWER_STATES.get(MoreObjects.firstNonNull(rhs.unwrap().powerState(), PowerState.UNKNOWN));
    }
  }

  private static class WithBasic implements Function<Void, WithOS> {
    private final AzureComputeInstanceTemplate template;
    private final BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever;
    private final LocalizationContext context;
    private final VirtualMachineScaleSets virtualMachineScaleSets;
    private final Networks networks;

    WithBasic(AzureComputeInstanceTemplate template,
              BiFunction<AzureComputeProviderConfigurationProperty, LocalizationContext, String> configRetriever,
              LocalizationContext context,
              VirtualMachineScaleSets virtualMachineScaleSets,
              Networks networks) {
      this.template = requireNonNull(template, "template is null");
      this.configRetriever = requireNonNull(configRetriever, "configRetriever is null");
      this.context = requireNonNull(context, "context is null");
      this.virtualMachineScaleSets = requireNonNull(virtualMachineScaleSets, "virtualMachineScaleSets is null");
      this.networks = requireNonNull(networks, "networks is null");
    }

    @Override
    public WithOS apply(Void dontcare) {
      String vnetResourceGroupName = template.getConfigurationValue(VIRTUAL_NETWORK_RESOURCE_GROUP, context);
      String vnetName = template.getConfigurationValue(VIRTUAL_NETWORK, context);
      String virtualMachineScaleSetName = getVirtualMachineScaleSetName(
          template.getInstanceNamePrefix(),
          template.getGroupId());

      Network network = networks.getByResourceGroup(vnetResourceGroupName, vnetName);
      requireNonNull(
          network,
          String.format(
              "network not found with resource group %s and network name %s",
              vnetResourceGroupName,
              vnetName));

      return virtualMachineScaleSets
          .define(virtualMachineScaleSetName)
          .withRegion(Region.findByLabelOrName(configRetriever.apply(REGION, context)))
          .withExistingResourceGroup(template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, context))
          .withSku(fromSkuNameAndTier(template.getConfigurationValue(VMSIZE, context), STANDARD_TIER))
          .withExistingPrimaryNetworkSubnet(network, template.getConfigurationValue(SUBNET_NAME, context))
          .withoutPrimaryInternetFacingLoadBalancer()
          .withoutPrimaryInternalLoadBalancer();
    }
  }

  private static class WithCustomImage implements Function<WithOS, WithLinuxCreateManaged> {
    private final AzureComputeInstanceTemplate template;
    private final LocalizationContext context;

    WithCustomImage(AzureComputeInstanceTemplate template, LocalizationContext context) {
      this.template = requireNonNull(template, "template is null");
      this.context = requireNonNull(context, "context is null");
    }

    @Override
    public WithLinuxCreateManaged apply(WithOS withOS) {
      WithLinuxCreateManaged result = requireNonNull(withOS, "withOS is null")
          .withLinuxCustomImage(template.getConfigurationValue(IMAGE, context))
          .withRootUsername(template.getConfigurationValue(SSH_USERNAME, context))
          .withSsh(template.getConfigurationValue(SSH_OPENSSH_PUBLIC_KEY, context));

      PurchasePlan plan = Configurations.parseCustomImagePurchasePlanFromConfig(template, context);
      if (plan == null) {
        LOG.info("no plan found for custom image");
      } else {
        ((VirtualMachineScaleSetImpl) result)
            .inner()
            .withPlan(
                new Plan()
                    .withName(plan.name())
                    .withProduct(plan.product())
                    .withPublisher(plan.publisher()));
      }

      return result;
    }
  }

  private static class WithNonCustomImage implements Function<WithOS, WithLinuxCreateManagedOrUnmanaged> {
    private final AzureComputeInstanceTemplate template;
    private final LocalizationContext context;
    private final VirtualMachineImages virtualMachineImages;

    WithNonCustomImage(
        AzureComputeInstanceTemplate template,
        LocalizationContext context,
        VirtualMachineImages virtualMachineImages) {
      this.template = requireNonNull(template, "template is null");
      this.context = requireNonNull(context, "context is null");
      this.virtualMachineImages = requireNonNull(virtualMachineImages, "virtualMachineImages is null");
    }

    @Override
    public WithLinuxCreateManagedOrUnmanaged apply(WithOS withOS) {
      ImageReference imageReference = Configurations.parseImageFromConfig(template, context);

      WithLinuxCreateManagedOrUnmanaged result = requireNonNull(withOS, "withOS is null")
          .withSpecificLinuxImageVersion(imageReference)
          .withRootUsername(template.getConfigurationValue(SSH_USERNAME, context))
          .withSsh(template.getConfigurationValue(SSH_OPENSSH_PUBLIC_KEY, context));

      VirtualMachineScaleSetInner inner = ((VirtualMachineScaleSetImpl) withOS).inner();
      PurchasePlan plan;

      if (Configurations.isPreviewImage(imageReference)) {
        plan = new PurchasePlan()
            .withName(imageReference.sku())
            .withProduct(imageReference.offer())
            .withPublisher(imageReference.publisher());
      } else {
        plan = virtualMachineImages.getImage(
            inner.location(),
            imageReference.publisher(),
            imageReference.offer(),
            imageReference.sku(),
            imageReference.version())
            .plan();
      }

      if (plan != null) {
        inner.withPlan(
            new Plan()
                .withName(plan.name())
                .withProduct(plan.product())
                .withPublisher(plan.publisher()));
      }
      return result;
    }
  }

  private static class WithManagedDisks<T extends WithManagedCreate> implements Function<T, WithCreate> {
    private final AzureComputeInstanceTemplate template;
    private final LocalizationContext context;

    WithManagedDisks(AzureComputeInstanceTemplate template, LocalizationContext context) {
      this.template = requireNonNull(template, "template is null");
      this.context = requireNonNull(context, "context is null");
    }

    @Override
    public WithCreate apply(T withManagedCreate) {
      requireNonNull(withManagedCreate, "withManagedCreate is null");
      // currently uniform data disks, no az designation
      Function<Integer, Function<WithManagedCreate, WithManagedCreate>> withDiskFunc = diskNum -> withCreate ->
          withCreate.withNewDataDisk(
              Integer.parseInt(template.getConfigurationValue(DATA_DISK_SIZE, context)));

      Function<WithManagedCreate, WithManagedCreate> withDisks = IntStream
          .range(0, Integer.parseInt(template.getConfigurationValue(DATA_DISK_COUNT, context)))
          .mapToObj(withDiskFunc::apply)
          .reduce(Function::andThen)
          .orElseGet(Function::identity);

      StorageAccountTypes storageAccountType = StorageAccountTypes.fromString(
          Configurations.convertStorageAccountTypeString(
              template.getConfigurationValue(STORAGE_TYPE, context)));

      return withDisks.apply(withManagedCreate)
          .withDataDiskDefaultStorageAccountType(storageAccountType)
          .withDataDiskDefaultCachingType(CachingTypes.NONE)
          .withOSDiskStorageAccountType(storageAccountType)
          .withOSDiskCaching(CachingTypes.READ_WRITE);
    }
  }

  private static class WithOtherConfigs implements Function<WithCreate, WithCreate> {
    private final AzureComputeInstanceTemplate template;
    private final LocalizationContext context;
    private final Identities identities;
    private final String subscriptionId;
    private final int count;

    WithOtherConfigs(
        AzureComputeInstanceTemplate template,
        LocalizationContext context,
        Identities identities,
        String subscriptionId,
        int count) {
      checkArgument(count >= 0, "negative minCount");
      this.template = requireNonNull(template, "template is null");
      this.context = requireNonNull(context, "context is null");
      this.identities = requireNonNull(identities, "identities is null");
      this.subscriptionId = requireNonNull(subscriptionId, "subscriptionId is null");
      this.count = count;
    }

    @Override
    public WithCreate apply(WithCreate withCreate) {
      requireNonNull(withCreate, "withCreate is null");

      withCreate.withTags(template.getTags())
          .withCapacity(count)
          .withCustomData(getBase64EncodedCustomData(
              template.getConfigurationValue(CUSTOM_DATA_UNENCODED, context),
              template.getConfigurationValue(CUSTOM_DATA_ENCODED, context)))
          .withComputerNamePrefix(getComputerNamePrefix(
              template.getInstanceNamePrefix(),
              template.getGroupId()));

      VirtualMachineScaleSetInner inner = ((VirtualMachineScaleSetImpl) withCreate).inner();
      if (count > 100) {
        inner.withSinglePlacementGroup(false);
      }

      VirtualMachineScaleSetNetworkConfiguration networkInterfaceConfiguration = inner
          .virtualMachineProfile()
          .networkProfile()
          .networkInterfaceConfigurations()
          .stream()
          .findFirst()
          .get();

      networkInterfaceConfiguration
          .withNetworkSecurityGroup(
              new SubResource()
                  .withId(
                      getId(
                          subscriptionId,
                          template.getConfigurationValue(NETWORK_SECURITY_GROUP_RESOURCE_GROUP, context),
                          ResourceProvider.VNET,
                          template.getConfigurationValue(NETWORK_SECURITY_GROUP, context))));

      if (template.getConfigurationValue(PUBLIC_IP, context).equalsIgnoreCase(TRUE)) {
        networkInterfaceConfiguration
            .ipConfigurations()
            .stream()
            .findFirst()
            .get()
            .withPublicIPAddressConfiguration(
                new VirtualMachineScaleSetPublicIPAddressConfiguration()
                    .withName(template.getGroupId())
                    .withIdleTimeoutInMinutes(PUBLIC_IP_IDLE_TIMEOUT_IN_MIN));
      }

      if (template.getConfigurationValue(WITH_ACCELERATED_NETWORKING, context).equalsIgnoreCase(TRUE)) {
        networkInterfaceConfiguration.withEnableAcceleratedNetworking(true);
      }

      String userAssignedMsiName = template.getConfigurationValue(USER_ASSIGNED_MSI_NAME, context);
      String userAssignedMsiRg = template.getConfigurationValue(USER_ASSIGNED_MSI_RESOURCE_GROUP, context);

      if (!StringUtils.isEmpty(userAssignedMsiName) && !StringUtils.isEmpty(userAssignedMsiRg)) {
        Identity identity = identities.getByResourceGroup(userAssignedMsiRg, userAssignedMsiName);
        withCreate.withExistingUserAssignedManagedServiceIdentity(identity);
      }

      return withCreate;
    }
  }

  private enum ResourceProvider {
    VMSS("Microsoft.Compute/virtualMachineScaleSets"),
    VNET("Microsoft.Network/networkSecurityGroups");

    String getName() {
      return name;
    }

    ResourceProvider(String name) {
      this.name = name;
    }

    private final String name;
  }
}
