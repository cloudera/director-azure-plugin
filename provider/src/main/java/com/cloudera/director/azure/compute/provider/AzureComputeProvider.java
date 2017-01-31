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

import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.IMAGE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.STORAGE_ACCOUNT_TYPE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VMSIZE;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_OPENSSH_PUBLIC_KEY;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationValidator;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.azure.utils.VmCreationParameters;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.Resource;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.SimpleInstanceState;
import com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.microsoft.azure.management.network.models.Subnet;
import com.microsoft.azure.management.network.models.VirtualNetwork;
import com.microsoft.azure.management.storage.models.AccountType;
import com.microsoft.azure.utility.ResourceContext;
import com.microsoft.windowsazure.exception.ServiceException;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.typesafe.config.ConfigException.Missing;
import com.typesafe.config.ConfigException.WrongType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider of compute resources (VMs) for Azure.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class AzureComputeProvider
  extends AbstractComputeProvider<AzureComputeInstance, AzureComputeInstanceTemplate> {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProvider.class);

  // Azure operation as well as the access token has life span of one hour
  private static final int TASKS_POLLING_TIMEOUT_SECONDS =
    Configurations.TASKS_POLLING_TIMEOUT_SECONDS;
  private static final int POLLING_INTERVAL_SECONDS = 10;
  // Initialize in constructor using value from azurePluginConfig
  public final int azureOperationPollingTimeout;

  private static final int RESOURCE_PREFIX_LENGTH = 8;

  // Resource name template, instance-id+random+type
  private static final String RESOURCE_NAME_TEMPLATE = "%s%s%s";

  private int lastSuccessfulAllocationCount = 0;
  private AzureCredentials credentials;
  private int lastSuccessfulDeletionCount = 0;

  private final ConfigurationValidator computeInstanceTemplateConfigValidator;

  /**
   * The provider configuration properties.
   */
  protected static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
    ConfigurationPropertiesUtil.asConfigurationPropertyList(
      AzureComputeProviderConfigurationProperty.values()
    );

  /**
   * The resource provider ID.
   */
  public static final String ID = AzureComputeProvider.class.getCanonicalName();

  /**
   * The resource provider metadata.
   */
  public static final ResourceProviderMetadata METADATA = SimpleResourceProviderMetadata.builder()
    .id(ID)
    .name("Azure")
    .description("Microsoft Azure compute provider")
    .providerClass(AzureComputeProvider.class)
    .providerConfigurationProperties(CONFIGURATION_PROPERTIES)
    .resourceTemplateConfigurationProperties(
      AzureComputeInstanceTemplate.getConfigurationProperties())
    .resourceDisplayProperties(AzureComputeInstance.getDisplayProperties())
    .build();


  public AzureComputeProvider(Configured configuration, AzureCredentials credentials,
    LocalizationContext localizationContext) {
    super(configuration, METADATA, localizationContext);
    this.credentials = credentials;
    String location = this.getConfigurationValue(AzureComputeProviderConfigurationProperty.REGION, localizationContext);
    this.computeInstanceTemplateConfigValidator =
      new AzureComputeInstanceTemplateConfigurationValidator(
        credentials, location);
    this.azureOperationPollingTimeout = getAzureOperationPollingTimeoutFromConfig();
  }

  @Override
  public ConfigurationValidator getResourceTemplateConfigurationValidator() {
    if (!AzurePluginConfigHelper.getValidateResourcesFlag()) {
      LOG.info("Skip all compute instance template configuration validator checks.");
      return super.getResourceTemplateConfigurationValidator();
    }
    return computeInstanceTemplateConfigValidator;
  }

  public void allocate(AzureComputeInstanceTemplate template, Collection<String> instanceIds,
    int minCount) throws InterruptedException {
    LOG.info("Starting to allocate the following instances {}.",instanceIds);

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();
    LocalizationContext providerLocalizationContext = getLocalizationContext();
    LocalizationContext templateLocalizationContext =
      SimpleResourceTemplate.getTemplateLocalizationContext(providerLocalizationContext);
    String adminName = template.getConfigurationValue(SSH_USERNAME, templateLocalizationContext);
    String sshPublicKey =
      template.getConfigurationValue(SSH_OPENSSH_PUBLIC_KEY, templateLocalizationContext);
    String location = this.getConfigurationValue(
      AzureComputeProviderConfigurationProperty.REGION, templateLocalizationContext);
    HashMap<String, String> tags =
      template.getTags().isEmpty() ? null : new HashMap<>(template.getTags());

    // AS is set per instance type. Per Cloudera Azure RA, master nodes will be in their own
    // master-only AS and so are the worker nodes.
    String computeRgName =
      template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, templateLocalizationContext);
    String availabilitySetName =
      template.getConfigurationValue(AVAILABILITY_SET, templateLocalizationContext);
    String vmSize =
      template.getConfigurationValue(VMSIZE, templateLocalizationContext);
    // storage account type has already been validated
    AccountType storageAccountType = AccountType.valueOf(
      template.getConfigurationValue(STORAGE_ACCOUNT_TYPE, templateLocalizationContext));
    int dataDiskCount =
      Integer.parseInt(template.getConfigurationValue(DATA_DISK_COUNT, templateLocalizationContext));
    int dataDiskSizeGiB =
      Integer.parseInt(template.getConfigurationValue(DATA_DISK_SIZE, templateLocalizationContext));
    String vnrgName =
      template.getConfigurationValue(VIRTUAL_NETWORK_RESOURCE_GROUP, templateLocalizationContext);
    String vnName =
      template.getConfigurationValue(VIRTUAL_NETWORK, templateLocalizationContext);
    String nsgrgName =
      template.getConfigurationValue(NETWORK_SECURITY_GROUP_RESOURCE_GROUP, templateLocalizationContext);
    String nsgName =
      template.getConfigurationValue(NETWORK_SECURITY_GROUP, templateLocalizationContext);
    boolean publicIpFlag =
      template.getConfigurationValue(PUBLIC_IP, templateLocalizationContext).equals("Yes");
    String fqdnSuffix =
      template.getConfigurationValue(HOST_FQDN_SUFFIX, templateLocalizationContext);
    String subnetName =
      template.getConfigurationValue(SUBNET_NAME, templateLocalizationContext);

    String image = template.getConfigurationValue(IMAGE, templateLocalizationContext);
    Config imageCfg;
    try {
      imageCfg = AzurePluginConfigHelper.getConfigurableImages().getConfig(image);
    } catch (Missing | WrongType e) {
      LOG.error("Failed to parse image details from plugin config.", e);
      throw new UnrecoverableProviderException(e);
    }
    String publisher = imageCfg.getString(Configurations.AZURE_IMAGE_PUBLISHER);
    String sku = imageCfg.getString(Configurations.AZURE_IMAGE_SKU);
    String offer = imageCfg.getString(Configurations.AZURE_IMAGE_OFFER);
    String version = imageCfg.getString(Configurations.AZURE_IMAGE_VERSION);

    AzureVmImageInfo imageInfo = new AzureVmImageInfo(publisher, sku, offer, version);

    VirtualNetwork vn;
    Subnet subnet;
    NetworkSecurityGroup nsg;
    AvailabilitySet as;

    // This AzureComputeProviderHelper should not be used by VM create tasks
    AzureComputeProviderHelper computeProviderHelper = credentials.getComputeProviderHelper();
    // Make sure all the required resources have been pre-configured and are present
    try {
      computeProviderHelper.getResourceGroup(computeRgName);
      computeProviderHelper.getResourceGroup(vnrgName);
      computeProviderHelper.getResourceGroup(nsgrgName);
      vn = computeProviderHelper.getVirtualNetworkByName(vnrgName, vnName);
      subnet = computeProviderHelper.getSubnetByName(vnrgName, vnName, subnetName);
      nsg = computeProviderHelper.getNetworkSecurityGroupByName(nsgrgName, nsgName);
      as = computeProviderHelper.getAvailabilitySetByName(computeRgName, availabilitySetName);
    } catch (IOException | ServiceException | URISyntaxException e) {
      LOG.error("Exception occurred while getting cluster wide resources, cannot proceed.", e);
      throw new UnrecoverableProviderException(e);
    }

    // Check if VM exists first before creating
    Collection<ResourceContext> contexts = new HashSet<>();
    Set<Future<TaskResult>> createVmTasks = new HashSet<>();
    Set<String> existingNames = new HashSet<>();
    Collection<AzureComputeInstance> vms = find(template, instanceIds);

    // Get names of existing VMs
    for (AzureComputeInstance vm : vms) {
      existingNames.add(vm.getId());
    }
    LOG.info("The following VMs already exists: {}.", existingNames);

    TaskRunner taskRunner = TaskRunner.build();

    // Create VMs in parallel.
    for (String instanceId : instanceIds) {
      // only create VM if the vm does not exist
      if (!existingNames.contains(instanceId)) {
        // One Azure ResourceContext per VM, context contain VM unique info such as IP and
        // storage account
        ResourceContext context = credentials.createResourceContext(location, computeRgName, publicIpFlag);
        context.setTags(tags);
        String resourcePrefix = instanceId.substring(0,8);
        String randomPadding = context.randomString(RESOURCE_PREFIX_LENGTH);

        context.setStorageAccountName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
          randomPadding, "sa"));
        context.setNetworkInterfaceName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
          randomPadding,  "nic"));
        context.setPublicIpName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
          randomPadding,  "publicip"));
        context.setContainerName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
          randomPadding,  "container"));
        context.setIpConfigName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
          randomPadding,  "ipconfig"));

        contexts.add(context);

        // each create task uses its own AzureComputeProviderHelper instance
        AzureComputeProviderHelper computeProviderHelperForTask =
            credentials.getComputeProviderHelper();

        VmCreationParameters parameters = new VmCreationParameters(vn, subnet, nsg, as,
          vmSize, template.getInstanceNamePrefix(), instanceId, fqdnSuffix, adminName,
          sshPublicKey, storageAccountType, dataDiskCount, dataDiskSizeGiB, imageInfo);

        Future<TaskResult> task = taskRunner.submitVmCreationTask(computeProviderHelperForTask,
            context, parameters, azureOperationPollingTimeout);
        createVmTasks.add(task);
      } else {
        LOG.info("VM {} already exists.", constructVmName(template, instanceId));
      }
    }

    Set<ResourceContext> failedContexts = new HashSet<>(contexts);

    // Wait for VMs to come up
    lastSuccessfulAllocationCount = taskRunner.pollPendingTasks(createVmTasks,
      TASKS_POLLING_TIMEOUT_SECONDS, POLLING_INTERVAL_SECONDS, failedContexts);

    LOG.info("Successfully allocated {} VMs.", lastSuccessfulAllocationCount);

    if (lastSuccessfulAllocationCount < minCount) {
      // Failed to reach minCount, delete all VMs and their supporting resources.
      LOG.info("Provisioned {} instances out of {}. minCount is {}. Delete all provisioned instances.",
        lastSuccessfulAllocationCount, instanceIds.size(), minCount);
      deleteResources(computeRgName, contexts, publicIpFlag);

      // We have less than minCount, which means the allocation has failed.
      // Therefore throw an UnrecoverableProviderException.
      PluginExceptionDetails pluginExceptionDetails =
        new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException(
        "Unrecoverable Error(s) occurred during instance creation.", pluginExceptionDetails);

    } else if (lastSuccessfulAllocationCount < instanceIds.size()) {
      // Cleanup resources created for failed VM allocations.
      deleteResources(computeRgName, failedContexts, publicIpFlag);
      LOG.info("Provisioned {} instances out of {}. minCount is {}.",
        lastSuccessfulAllocationCount, instanceIds.size(), minCount);
    }

    // More than minCount VMs have successfully been provisioned, allocation is consider successful

    // Remove all of the failed VMs from contexts
    for (ResourceContext failed : failedContexts) {
      contexts.remove(failed);
    }

    // Log the overall template info for this batch
    LOG.info("VM Template info for this batch: " +
      "Batch Size: {}; " +
      "Succeeded: {}; " +
      "Failed: {}; " +
      "VM Size: {}; " +
      "Image Info: {}; " +
      "Compute Resource Group Name: {}; " +
      "Virtual Network Resource Group Name: {}; " +
      "Virtual Network Name: {}; " +
      "Subnet Name: {}; " +
      "Host FQDN Suffix: {}; " +
      "Network Security Group Resource Group Name: {}; " +
      "Network Security Group Name: {}; " +
      "Public IP: {}; " +
      "Availability Set Name: {}; " +
      "Data Disk Count: {}; " +
      "Data Disk Size in GiB: {}; ",
      contexts.size() + failedContexts.size(),
      contexts.size(),
      failedContexts.size(),
      vmSize,
      imageInfo,
      computeRgName,
      vnrgName,
      vnName,
      subnetName,
      fqdnSuffix,
      nsgrgName,
      nsgName,
      publicIpFlag,
      availabilitySetName,
      dataDiskCount,
      dataDiskSizeGiB
    );

    // Log the individual VMs for easier correlation for debugging
    logContexts(contexts, fqdnSuffix, "succeeded");
    logContexts(failedContexts, fqdnSuffix, "failed");

    // just log any error messages and exit
    if (accumulator.hasError()) {
      logErrorMessage(accumulator);
    }
  }

  /**
   * Logs all of an individual VMs resources together to allow easier correlation for debugging.
   *
   * @param contexts the VMs to log
   * @param fqdnSuffix used for logging logic
   * @param status used for logging text
   */
  private void logContexts(Collection<ResourceContext> contexts, String fqdnSuffix, String status) {
    for (ResourceContext context : contexts) {
      // These assignments can throw NullPointerExceptions
      String vmName;
      String internalFQDN;
      try {
        // vmName is in the form userDefinedPrefix-UUID
        // E.g. master-7da97fd2-9d6b-4e31-a926-f9d4721656a2
        vmName = context.getVMInput().getName();

        // internalFQDN is in the form userDefinedPrefix-firstEightCharactersOfUUID.fqdnSuffix
        // E.g. master-7da97fd2.cdh-cluster.internal
        // The number 28 is used here to get cut off the last 28 characters of the UUID. Given that
        // a UUID has 36 characters (including dashes) and the vnName ends with a UUID this will
        // return the user defined portion and the first 8 characters (36 - 28 = 8) of the UUID
        internalFQDN = vmName.substring(0, vmName.length() - 28) + "." + fqdnSuffix;
      } catch (NullPointerException e) {
        vmName = null;
        internalFQDN = null;
      }
      // these will always be set
      String niName = context.getNetworkInterfaceName();
      String pipName = context.getPublicIpName();
      String saName = context.getStorageAccountName();

      LOG.info("Virtual Machine allocation " + status + " for VM: " +
        "VM Name: {}; " +
        "Host Internal FQDN: {}; " +
        "Network Interface Name: {}; " +
        "Public IP Name: {}; " +
        "Storage Account Name: {};",
        vmName,
        internalFQDN,
        niName,
        pipName,
        saName
      );
    }
  }

  /**
   * Creates a name for the VM. VM name = [instance prefix]-[instance id] .
   *
   * @param template   instance template
   * @param instanceId instance Id, provided by Director server
   * @return the combined VM name
   */
  private String constructVmName(AzureComputeInstanceTemplate template, String instanceId) {
    return template.getInstanceNamePrefix() + "-" + instanceId;
  }

  private void logErrorMessage(PluginExceptionConditionAccumulator accumulator) {
    Map<String, Collection<PluginExceptionCondition>> conditionsByKeyMap =
      accumulator.getConditionsByKey();

    for (Map.Entry<String, Collection<PluginExceptionCondition>> keyToCondition : conditionsByKeyMap.entrySet()) {
      String key = keyToCondition.getKey();
      if (key != null) {
        for (PluginExceptionCondition condition : keyToCondition.getValue()) {
          LOG.info("({}) {}: {}", condition.getType(), key, condition.getMessage());
        }
      } else {
        for (PluginExceptionCondition condition : keyToCondition.getValue()) {
          LOG.info("({}) {}", condition.getType(), condition.getMessage());
        }
      }
    }
  }

  @Override
  public Collection<AzureComputeInstance> find(AzureComputeInstanceTemplate template,
    Collection<String> instanceIds) throws InterruptedException {
    LOG.info("Trying to find the following VMs: {}.", instanceIds);

    String rgName = getResourceGroupFromTemplate(template);

    AzureComputeProviderHelper computeProviderHelper = credentials.getComputeProviderHelper();

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();
    List<AzureComputeInstance> result = new ArrayList<AzureComputeInstance>();
    Map<String, VirtualMachine> prefixedNameToVmMapping = listVmsInResourceGroup(rgName,
        accumulator);
    for (String currentId : instanceIds) {
      try {
        String key = constructVmName(template, currentId);
        VirtualMachine vm = prefixedNameToVmMapping.get(key);
        if (vm != null) {
          result.add(new AzureComputeInstance(
            template, currentId, computeProviderHelper.createAzureComputeInstanceHelper(vm,
            credentials, rgName)));
        }
      } catch (IOException | ServiceException e) {
        LOG.info("Instance '{}' not found due to error.", currentId, e);
        accumulator.addError(null, e.getMessage());
      }
    }

    LOG.info("Found the following VMs: {}.", result);

    if (accumulator.hasError()) {
      logErrorMessage(accumulator);
    }

    return result;
  }

  /**
   * Helper function to delete VM and its supporting resources.
   * <p/>
   * Blocks until resources are deleted or an error is encountered. Interruption to the clean up
   * process is considered an error and an exception will be thrown.
   *
   * @param resourceGroup resource group name
   * @param contexts      Azure context populated during VM allocation
   * @param isPublicIPConfigured
   */
  private void deleteResources(String resourceGroup, Collection<ResourceContext> contexts,
    boolean isPublicIPConfigured) {
    LOG.info("Tearing down resources within resource group: {}.", resourceGroup);
    AzureComputeProviderHelper computeProviderHelperForTask = credentials.getComputeProviderHelper();
    TaskRunner taskRunner = TaskRunner.build();
    try {
      taskRunner.submitAndRunResourceDeleteTasks(computeProviderHelperForTask, resourceGroup,
          contexts, isPublicIPConfigured, azureOperationPollingTimeout);
    } catch (InterruptedException e) {
      String errMsg = "Resource cleanup is interrupted. There may be resources left not cleaned up."
        + " Please check Azure portal to make sure remaining resources are deleted.";
      LOG.error(errMsg + " Resource details: {}.", contexts, e);
      throw new UnrecoverableProviderException(errMsg, e);
    }
  }

  public Resource.Type getResourceType() {
    return AbstractComputeInstance.TYPE;
  }

  @Override
  public Map<String, InstanceState> getInstanceState(AzureComputeInstanceTemplate template,
    Collection<String> instanceIds) {
    LOG.info("Getting state for the following instances (instance name prefix '{}'): {}.",
      template.getInstanceNamePrefix(), instanceIds);

    String rgName = getResourceGroupFromTemplate(template);
    HashMap<String, InstanceState> states = new HashMap<>();
    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();
    AzureComputeProviderHelper computeProviderHelper = credentials.getComputeProviderHelper();

    Map<String, VirtualMachine> prefixedNameToVmMapping = listVmsInResourceGroup(rgName,
        accumulator);
    for (String currentId : instanceIds) {
      boolean found = false;
      try {
        String key = constructVmName(template, currentId);
        VirtualMachine vm = prefixedNameToVmMapping.get(key);
        if (vm != null) {
          states.put(currentId, computeProviderHelper.getVirtualMachineStatus(rgName, vm.getName()));
          found = true;
        }
      } catch (ServiceException | URISyntaxException | IOException e) {
        accumulator.addError(null, e.getMessage());
        LOG.error("Get state for VM {} encountered error.", currentId, e);
      }
      if (!found) {
        states.put(currentId, new SimpleInstanceState(InstanceStatus.UNKNOWN));
      }
    }

    if (accumulator.hasError()) {
      logErrorMessage(accumulator);
    }

    return states;
  }

  /**
   * Lists all VMs in the resource group.
   *
   * @param rgName name of the Azure ResourceGroup to list the VMs
   * @param accumulator error accumulator
   * @return a map contains VM name, VM object pair. In case of any Exception was thrown, the map
   * will be empty
   */
  private Map<String, VirtualMachine> listVmsInResourceGroup(
    String rgName, PluginExceptionConditionAccumulator accumulator) {
    HashMap<String, VirtualMachine> prefixedNameToVmMapping = new HashMap<>();
    AzureComputeProviderHelper computeProviderHelper = credentials.getComputeProviderHelper();
    try {
      for (VirtualMachine vm : computeProviderHelper.getVirtualMachines(rgName)) {
        prefixedNameToVmMapping.put(vm.getName(), vm);
      }
    } catch (ServiceException | URISyntaxException | IOException e) {
      LOG.info("Can't list VM in resource group '{}' due to error: ", e);
      accumulator.addError(null, e.getMessage());
    }
    return prefixedNameToVmMapping;
  }

  @Override
  public void delete(AzureComputeInstanceTemplate template, Collection<String> instanceIds) throws
    InterruptedException {
    boolean isPublicIpConfigured = getPublicIpFlagFromTemplate(template);
    LOG.info("Deleting the following VMs (VM name prefix is '" + template.getInstanceNamePrefix()
      + "'): " + instanceIds);
    String rgName = getResourceGroupFromTemplate(template);
    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    Map<String, VirtualMachine> prefixedNameToVmMapping = listVmsInResourceGroup(rgName,
        accumulator);
    Set<Future<TaskResult>> deleteVmTasks = new HashSet<>();
    TaskRunner taskRunner = TaskRunner.build();
    for (String currentId : instanceIds) {
      String key = constructVmName(template, currentId);
      VirtualMachine vm = prefixedNameToVmMapping.get(key);
      if (vm != null) {
        LOG.debug("Sending delete request to Azure for VM: {}.", vm.getName());
        AzureComputeProviderHelper computeProviderHelperForTask = credentials.getComputeProviderHelper();
        deleteVmTasks.add(taskRunner.submitDeleteVmTask(computeProviderHelperForTask, rgName, vm,
          isPublicIpConfigured, azureOperationPollingTimeout));
      }
    }
    // wait for VM to be deleted
    lastSuccessfulDeletionCount = taskRunner.pollPendingTasks(deleteVmTasks,
      TASKS_POLLING_TIMEOUT_SECONDS,
      POLLING_INTERVAL_SECONDS, null);

    LOG.info("Successfully deleted {} VMs.", lastSuccessfulDeletionCount);

    if (accumulator.hasError()) {
      logErrorMessage(accumulator);
      PluginExceptionDetails pluginExceptionDetails =
        new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException("Error occurred during instance deletion.",
        pluginExceptionDetails);
    }
  }

  private boolean getPublicIpFlagFromTemplate(AzureComputeInstanceTemplate template) {
    return template.getConfigurationValue(PUBLIC_IP,
      SimpleResourceTemplate.getTemplateLocalizationContext(getLocalizationContext()))
      .equals("Yes");
  }

  public AzureComputeInstanceTemplate createResourceTemplate(
    String name, Configured configuration, Map<String, String> tags) {
    return new AzureComputeInstanceTemplate(name, configuration, tags, getLocalizationContext());
  }

  public int getLastSuccessfulAllocationCount() {
    return lastSuccessfulAllocationCount;
  }

  public int getLastSuccessfulDeletionCount() {
    return lastSuccessfulDeletionCount;
  }

  private String getResourceGroupFromTemplate(AzureComputeInstanceTemplate template) {
    LocalizationContext providerLocalizationContext = getLocalizationContext();
    LocalizationContext templateLocalizationContext =
      SimpleResourceTemplate.getTemplateLocalizationContext(providerLocalizationContext);
    return template.getConfigurationValue(COMPUTE_RESOURCE_GROUP, templateLocalizationContext);
  }

  private int getAzureOperationPollingTimeoutFromConfig() {
    return AzurePluginConfigHelper.getAzurePluginConfigProviderSection()
      .getInt(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS);
  }
}
