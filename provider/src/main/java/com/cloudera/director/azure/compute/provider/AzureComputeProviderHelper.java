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

package com.cloudera.director.azure.compute.provider;

import static com.sun.xml.bind.v2.util.ClassLoaderRetriever.getClassLoader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceHelper;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.azure.utils.AzureVirtualMachineState;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.exception.TransientProviderException;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.SimpleInstanceState;
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.microsoft.azure.management.compute.models.AvailabilitySetGetResponse;
import com.microsoft.azure.management.compute.models.CachingTypes;
import com.microsoft.azure.management.compute.models.ComputeLongRunningOperationResponse;
import com.microsoft.azure.management.compute.models.ComputeOperationResponse;
import com.microsoft.azure.management.compute.models.DataDisk;
import com.microsoft.azure.management.compute.models.DeleteOperationResponse;
import com.microsoft.azure.management.compute.models.DiskCreateOptionTypes;
import com.microsoft.azure.management.compute.models.ImageReference;
import com.microsoft.azure.management.compute.models.InstanceViewStatus;
import com.microsoft.azure.management.compute.models.LinuxConfiguration;
import com.microsoft.azure.management.compute.models.OSProfile;
import com.microsoft.azure.management.compute.models.Plan;
import com.microsoft.azure.management.compute.models.PurchasePlan;
import com.microsoft.azure.management.compute.models.SshConfiguration;
import com.microsoft.azure.management.compute.models.SshPublicKey;
import com.microsoft.azure.management.compute.models.VirtualHardDisk;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.compute.models.VirtualMachineCreateOrUpdateResponse;
import com.microsoft.azure.management.compute.models.VirtualMachineExtension;
import com.microsoft.azure.management.compute.models.VirtualMachineImage;
import com.microsoft.azure.management.compute.models.VirtualMachineImageGetParameters;
import com.microsoft.azure.management.compute.models.VirtualMachineSizeListResponse;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.models.AzureAsyncOperationResponse;
import com.microsoft.azure.management.network.models.IpAllocationMethod;
import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.NetworkInterfaceGetResponse;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.microsoft.azure.management.network.models.OperationStatus;
import com.microsoft.azure.management.network.models.PublicIpAddress;
import com.microsoft.azure.management.network.models.PublicIpAddressDnsSettings;
import com.microsoft.azure.management.network.models.ResourceId;
import com.microsoft.azure.management.network.models.Subnet;
import com.microsoft.azure.management.network.models.VirtualNetwork;
import com.microsoft.azure.management.resources.ResourceManagementClient;
import com.microsoft.azure.management.resources.ResourceManagementService;
import com.microsoft.azure.management.resources.models.ResourceGroupExtended;
import com.microsoft.azure.management.storage.StorageManagementClient;
import com.microsoft.azure.management.storage.StorageManagementService;
import com.microsoft.azure.management.storage.models.AccountType;
import com.microsoft.azure.management.storage.models.StorageAccount;
import com.microsoft.azure.management.storage.models.StorageAccountCreateParameters;
import com.microsoft.azure.management.storage.models.StorageAccountUpdateParameters;
import com.microsoft.azure.management.storage.models.StorageAccountUpdateResponse;
import com.microsoft.azure.utility.ComputeHelper;
import com.microsoft.azure.utility.NetworkHelper;
import com.microsoft.azure.utility.ResourceContext;
import com.microsoft.azure.utility.StorageHelper;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.OperationResponse;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to hide the details of interacting with Azure SDK.
 */
@SuppressWarnings("PMD.UnusedPrivateMethod")
public class AzureComputeProviderHelper {

  // NOTE: APIs exposed by Azure clients below are not assumed to be thread safe.
  private ResourceManagementClient resourceManagementClient;
  private StorageManagementClient storageManagementClient;
  private ComputeManagementClient computeManagementClient;
  private NetworkResourceProviderClient networkResourceProviderClient;

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProviderHelper.class);

  private static final String LATEST = "latest";

  public AzureComputeProviderHelper(Configuration azureConfig) {
    this.resourceManagementClient = ResourceManagementService.create(azureConfig);
    this.storageManagementClient = StorageManagementService.create(azureConfig);
    this.computeManagementClient = ComputeManagementService.create(azureConfig);
    this.networkResourceProviderClient = NetworkResourceProviderService.create(azureConfig);
  }

  /**
   * Private empty constructor and setter methods for testing.
   */
  private AzureComputeProviderHelper() {
  }

  /**
   * Helper function to shorten VM name (32-char UUID provided by director).
   * <p/>
   * NOTE: This is used to generated FQDN for both public DNS label and VM host names. Cloudera
   * requires using FQDN as host names.
   *
   * @param vmNamePrefix User inputted VM name prefix
   * @param instanceId   UUID as a string
   * @return shortened VM name
   * @see <a href="http://www.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_networknames_configure.html" />
   */
  public static String getShortVMName(String vmNamePrefix, String instanceId) {
    return vmNamePrefix + "-" + instanceId.substring(0, 8);
  }

  /**
   * Creates an array of data disk objects.
   *
   * @param dataDiskCount # of data disks to be created
   * @param sizeInGB      size of the data disks to be created
   * @param vhdContainer  VHD container URL
   * @return an array of data disk objects to attach to VM object
   */
  public ArrayList<DataDisk> createDataDisks(int dataDiskCount, int sizeInGB, String vhdContainer) {
    ArrayList<DataDisk> dataDisks = new ArrayList(dataDiskCount);
    //allocate a data disk for logging
    //this is addition to the dataDiskCount

    for (int i = 0; i < dataDiskCount; i++) {
      DataDisk disk = new DataDisk();
      disk.setCreateOption(DiskCreateOptionTypes.EMPTY);
      disk.setDiskSizeGB(sizeInGB);

      VirtualHardDisk vhd = disk.getVirtualHardDisk();
      if (vhd == null) {
        vhd = new VirtualHardDisk();
        disk.setVirtualHardDisk(vhd);
      }

      // Autogenerate name if needed
      if (disk.getName() == null) {
        disk.setName("disk" + i);
      }

      // Autogenerate LUN if needed
      if (disk.getLun() == 0) {
        disk.setLun(i);
      }

      // Assume no caching if not set
      if (disk.getCaching() == null) {
        disk.setCaching(CachingTypes.NONE);
      }

      // Autogenerate URI from name
      if (vhd.getUri() == null) {
        String dataDiskUri = vhdContainer + String.format("/%s.vhd", disk.getName());
        vhd.setUri(dataDiskUri);
      }
      dataDisks.add(disk);
    }

    return dataDisks;
  }

  public synchronized ComputeLongRunningOperationResponse getLongRunningOperationStatus(
    String azureAsyncOperation) throws IOException, ServiceException {
    return computeManagementClient.getLongRunningOperationStatus(azureAsyncOperation);
  }

  protected synchronized VirtualMachineCreateOrUpdateResponse submitVmCreationOp(
    ResourceContext context) throws ServiceException, IOException, URISyntaxException {
    LOG.info("Begin creating VM: {}.", context.getVMInput().getName());
    return computeManagementClient.getVirtualMachinesOperations()
      .beginCreatingOrUpdating(
        context.getResourceGroupName(),
        context.getVMInput()
      );
  }

  /**
   * Creates custom scripts (Azure VM Extensions) to be run when VM is created.
   * <p>
   * NOTE: Testing only. We do not use this feature in the plugin.
   *
   * @param context Azure ResourceContext
   * @return response of the create operation
   * @throws IOException
   * @throws ServiceException
   * @throws URISyntaxException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public synchronized ComputeOperationResponse createCustomizedScript(ResourceContext context)
    throws IOException, ServiceException, URISyntaxException, ExecutionException,
    InterruptedException {
    VirtualMachineExtension vme = new VirtualMachineExtension();
    vme.setName("prepare");

    // AZURE_SDK
    ClassLoader classLoader = getClassLoader();
    String result;
    result = IOUtils.toString(classLoader.getResourceAsStream("placeholder.json"));

    vme.setLocation(context.getLocation());
    vme.setPublisher("Microsoft.OSTCExtensions");
    vme.setExtensionType("CustomScriptForLinux");
    vme.setTypeHandlerVersion("1.2");
    vme.setSettings(result);
    return computeManagementClient.getVirtualMachineExtensionsOperations().beginCreatingOrUpdating(
      context.getResourceGroupName(), context.getVMInput().getName(), vme);

  }

  /**
   * Returns the purchase plan, if it exists, from Azure.
   *
   * @param vmImage contains Virtual Machine Image information, built from Azure
   * @param imageInfo contains the image content, built from images.conf
   * @return a Plan from Azure, or null if no plan is found
   */
  public Plan getPlan(VirtualMachineImage vmImage, AzureVmImageInfo imageInfo) {
    Plan plan = new Plan();

    // Set the purchase plan if the image has one attached. Certain images do not have a plan
    PurchasePlan purchasePlan = vmImage.getPurchasePlan();
    if (purchasePlan != null) {
      LOG.info("Image {} has a purchase plan attached.", imageInfo);
      plan.setName(purchasePlan.getName());
      plan.setProduct(purchasePlan.getProduct());
      plan.setPromotionCode(null);
      plan.setPublisher(purchasePlan.getPublisher());
    } else {
      LOG.info("Image {} does not have a purchase plan attached.", imageInfo);
      return null;
    }

    return plan;
  }

  /**
   * Queries Azure to get information of a VM image published on Azure Marketplace.
   *
   * @param location  Azure region
   * @param imageInfo image information
   * @return an Azure image object if the image is successfully located
   * @throws ServiceException   when Azure backend returns some error
   * @throws IOException        when there is some network error
   * @throws URISyntaxException when an invalid URI is given
   */
  public synchronized VirtualMachineImage getMarketplaceVMImage(String location,
    AzureVmImageInfo imageInfo)
    throws ServiceException, IOException, URISyntaxException {
    String publisher = imageInfo.getPublisher();
    String offer = imageInfo.getOffer();
    String sku = imageInfo.getSku();
    String version = imageInfo.getVersion();
    ImageReference imageRef = ComputeHelper.getDefaultVMImage(computeManagementClient, location,
      publisher, offer, sku);
    VirtualMachineImageGetParameters param = new VirtualMachineImageGetParameters();
    param.setLocation(location);
    param.setPublisherName(publisher);
    param.setOffer(offer);
    param.setSkus(sku);
    if (version.equals(LATEST)) {
      version = imageRef.getVersion();
    }
    param.setVersion(version);

    return computeManagementClient.getVirtualMachineImagesOperations().get(param)
      .getVirtualMachineImage();
  }

  /**
   * Checks if AvailabilitySet is already created. If not create one and set it to context. If so
   * use the existing AvailabilitySet and update Resource Context.
   * <p>
   * NOTE: this method is used for testing only. Director Azure Plugin doesn't create AS on user's
   * behalf.
   *
   * @param context Azure ResourceContext
   * @return AvailabilitySet ID
   * @throws ServiceException
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  public synchronized String createAndSetAvailabilitySetId(ResourceContext context)
    throws ServiceException, ExecutionException, InterruptedException, IOException {
    String asName = context.getAvailabilitySetName();
    String location = context.getLocation();
    String resourceGroup = context.getResourceGroupName();
    String subscriptionId = context.getSubscriptionId();
    HashMap<String, String> asTags = context.getTags();


    AvailabilitySet as = null;
    try {
      AvailabilitySetGetResponse response =
        computeManagementClient.getAvailabilitySetsOperations().get(
          context.getResourceGroupName(), context.getAvailabilitySetName());
      as = response.getAvailabilitySet();
    } catch (IOException | ServiceException | URISyntaxException e) {
      LOG.error("Get AvailabilitySet {} from Azure encountered error: ",
        context.getAvailabilitySetName(), e);
    }

    if (as == null) {
      as = new AvailabilitySet(location);
      LOG.debug("Creating new Availability Set: {}.", asName);
    }
    as.setName(asName);

    if (asTags != null) {
      as.setTags(asTags);
      LOG.debug("Availability Set tags: {}.", asTags);
    }

    // NO-OP if as name already exists
    computeManagementClient.getAvailabilitySetsOperations().createOrUpdate(resourceGroup, as);

    String availabilitySetId = ComputeHelper.getAvailabilitySetRef(
      subscriptionId, context.getResourceGroupName(), asName);
    context.setAvailabilitySetId(availabilitySetId);
    return availabilitySetId;
  }

  /**
   * Creates a public IP resource, then assign DNS label, forward and reverse FQDN to the public IP
   * resource.
   *
   * AZURE_SDK This is a re-iteration of NetworkHelper.createPublicIpAddress() in Azure SDK.
   * The SDK function does not set tags, DNS label, forward and reverse FQDN.
   *
   * @param context   Azure ResourceContext
   * @param dnsLabel  Public DNS label
   * @return PublicIpAddress object
   * @throws InterruptedException when polling for Azure backend operation result is interrupted
   * @throws ExecutionException   when underlying Azure backend call throws an exception
   * @throws IOException          when there's some IO error
   * @throws ServiceException     when Azure backend returns some error
   */
  private PublicIpAddress createPublicIpAddress(ResourceContext context, String dnsLabel)
      throws InterruptedException, ExecutionException, IOException, ServiceException {
    String publicIpName = context.getPublicIpName();
    PublicIpAddress publicIpParams = new PublicIpAddress(IpAllocationMethod.DYNAMIC,
        context.getLocation());
    PublicIpAddressDnsSettings dnsSettings = new PublicIpAddressDnsSettings();
    dnsSettings.setDomainNameLabel(dnsLabel);
    publicIpParams.setTags(context.getTags());
    publicIpParams.setDnsSettings(dnsSettings);

    AzureAsyncOperationResponse resp = networkResourceProviderClient
        .getPublicIpAddressesOperations()
        .createOrUpdate(context.getResourceGroupName(), publicIpName, publicIpParams);
    if (!resp.getStatus().equals(OperationStatus.SUCCEEDED)) {
      throw new TransientProviderException("Failed to create Public IP " +
          publicIpParams.getName() + " status code: " + resp.getStatusCode() + ".");
    }

    PublicIpAddress publicIpCreated = networkResourceProviderClient.getPublicIpAddressesOperations()
        .get(context.getResourceGroupName(), publicIpName).getPublicIpAddress();
    context.setPublicIpAddress(publicIpCreated);

    return setReverseFqdnForPublicIp(context);
  }

  /**
   * Sets reverse FQDN for a public IP. This function assumes a DNS label is already assigned to the
   * public IP resource.
   *
   * @param context Azure ResourceContext
   * @return PublicIpAddress object
   * @throws InterruptedException when polling for Azure backend operation result is interrupted
   * @throws ExecutionException   when underlying Azure backend call throws an exception
   * @throws IOException          when there's some IO error
   * @throws ServiceException     when Azure backend returns some error
   */
  private PublicIpAddress setReverseFqdnForPublicIp(ResourceContext context)
      throws IOException, ServiceException, InterruptedException, ExecutionException {
    PublicIpAddress publicIp = context.getPublicIpAddress();
    if (publicIp == null) {
      throw new UnrecoverableProviderException("Failed to get public IP config for Public IP "
          + context.getPublicIpName() + ".");
    }

    PublicIpAddressDnsSettings dnsSettings = publicIp.getDnsSettings();
    if (dnsSettings == null) {
      throw new UnrecoverableProviderException("DNS settings is missing for Public IP " +
          publicIp.getName() + ".");
    }

    dnsSettings.setReverseFqdn(dnsSettings.getFqdn());
    publicIp.setDnsSettings(dnsSettings);

    // update public IP setting
    AzureAsyncOperationResponse resp = networkResourceProviderClient
        .getPublicIpAddressesOperations().createOrUpdate(
            context.getResourceGroupName(), context.getPublicIpName(), publicIp);
    if (!resp.getStatus().equals(OperationStatus.SUCCEEDED)) {
      throw new UnrecoverableProviderException("Failed to set reverse FQDN for Public IP "
          + publicIp.getName() + " status code: " + resp.getStatusCode() + ".");
    }

    PublicIpAddress publicIpUpdated = networkResourceProviderClient.getPublicIpAddressesOperations()
        .get(context.getResourceGroupName(), context.getPublicIpName()).getPublicIpAddress();
    context.setPublicIpAddress(publicIpUpdated);

    LOG.info("Successfully set reverse FQDN to {} for Public IP {}.",
        publicIpUpdated.getDnsSettings().getReverseFqdn(), context.getPublicIpName());

    return publicIpUpdated;
  }

  /**
   * Creates Public IP (if configured) and NIC.
   *
   * @param context   Azure ResourceContext
   * @param snet      Azure Subnet object
   * @param publicDnsLabel  Public DNS label
   * @return NetworkInterface resource created in Azure
   * @throws Exception Azure SDK API calls throw generic exceptions
   */
  public synchronized NetworkInterface createAndSetNetworkInterface(ResourceContext context,
      Subnet snet, String publicDnsLabel) throws Exception {
    if (context.isCreatePublicIpAddress() && context.getPublicIpAddress() == null) {
      PublicIpAddress pip = createPublicIpAddress(context, publicDnsLabel);
      LOG.info("Successfully created Public IP {}", pip.getName());
    }

    LOG.info("Using subnet {} under VNET {}.", snet.getName(),
      context.getVirtualNetwork().getName());

    return NetworkHelper.createNIC(networkResourceProviderClient, context, snet);
  }

  /**
   * Creates a premium StorageAccount for the VM.
   *
   * @param storageAccountType type of storage account to create
   * @param context Azure ResourceContext
   * @return StorageAccount resource created in Azure
   * @throws Exception
   */
  public synchronized StorageAccount createAndSetStorageAccount(AccountType storageAccountType,
    ResourceContext context) throws Exception {
    StorageAccountCreateParameters stoInput = new StorageAccountCreateParameters(storageAccountType,
      context.getLocation());
    // AZURE_SDK StorageHelper.createStorageAccount() throws generic Exception.
    // Create the storage account
    StorageHelper.createStorageAccount(storageManagementClient, context, stoInput);
    // AZURE_SDK: We're doing the extra round trip because createStorageAccount does not set all of
    // fields we need before returning - this forces a "refresh" and gets all the fields
    StorageAccount sa = storageManagementClient
      .getStorageAccountsOperations()
      .getProperties(context.getResourceGroupName(), context.getStorageAccountName())
      .getStorageAccount();

    if (sa.getPrimaryEndpoints() != null && sa.getPrimaryEndpoints().getBlob() != null) {
      LOG.info("StorageAccount: {} has blob URI: {}", sa.getName(),
        sa.getPrimaryEndpoints().getBlob());
    } else {
      LOG.info("StorageAccount: {} has no blob URI.", sa.getName());
    }

    context.setStorageAccount(sa);

    // AZURE_SDK StorageHelper.createStorageAccount() does not pick up tags from context
    if (context.getTags() != null) {
      LOG.info("Adding tags to StorageAccount: {}", sa.getName());
      sa.setTags(context.getTags());
      StorageAccountUpdateParameters saUpdateParams = new StorageAccountUpdateParameters();
      saUpdateParams.setTags(context.getTags());
      StorageAccountUpdateResponse resp =
        storageManagementClient.getStorageAccountsOperations().update(
          context.getResourceGroupName(), sa.getName(), saUpdateParams);
      if (resp.getStatusCode() != HttpStatus.SC_OK) {
        throw new TransientProviderException(
          "Failed to add tags to StorageAccount " + sa.getName() + " status code " +
            resp.getStatusCode() + ".");
      }
      context.setStorageAccount(sa);
      LOG.debug("Successfully updated tags for StorageAccount {}.", sa.getName());
    }
    return sa;
  }

  /**
   * Sets a NetworkSecurityGroup to a NetworkInterface in Azure.
   *
   * @param nsg     NetworkSecurityGroup object
   * @param context Azure ResourceContext object, contains NetworkInterface info
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public synchronized void setNetworkSecurityGroup(NetworkSecurityGroup nsg,
    ResourceContext context) throws IOException, InterruptedException, ExecutionException {
    ResourceId id = new ResourceId();
    id.setId(nsg.getId());
    context.getNetworkInterface().setNetworkSecurityGroup(id);
    AzureAsyncOperationResponse resp =
      networkResourceProviderClient.getNetworkInterfacesOperations().createOrUpdate(
        context.getResourceGroupName(), context.getNetworkInterfaceName(),
        context.getNetworkInterface());
    if (resp.getStatusCode() != HttpStatus.SC_OK) {
      throw new TransientProviderException(
        "Failed to add tags to NetworkSecurityGroup " + nsg.getName() + " status code " +
          resp.getStatusCode() + ".");
    }
  }

  /**
   * Creates a resource group (in region specified in ResourceContext).
   * <p>
   * NOTE: Testing only. We do not use this feature in the plugin.
   *
   * @param context Azure ResourceContext object, contains info of the resource group to be created
   * @throws IOException
   * @throws ServiceException
   * @throws URISyntaxException
   */
  public void createOrUpdateResourceGroup(ResourceContext context) throws IOException,
    ServiceException, URISyntaxException {
    ComputeHelper.createOrUpdateResourceGroup(resourceManagementClient, context);
  }

  /**
   * Creates a Linux config object containing SSH public key. This is used for VM creation.
   *
   * @param osProfile    Azure operating system profile object
   * @param sshPublicKey SSH public key
   * @return a Linux config object containing SSH public key.
   */
  public static LinuxConfiguration createSshLinuxConfig(OSProfile osProfile, String sshPublicKey) {
    LinuxConfiguration linuxConfig = new LinuxConfiguration();
    SshConfiguration sshConfig = new SshConfiguration();

    ArrayList<SshPublicKey> listOfKeys = new ArrayList<SshPublicKey>();
    SshPublicKey publicKey = new SshPublicKey();
    publicKey.setKeyData(sshPublicKey);
    publicKey.setPath(getSshPath(osProfile.getAdminUsername()));

    listOfKeys.add(publicKey);
    sshConfig.setPublicKeys(listOfKeys);
    linuxConfig.setSshConfiguration(sshConfig);
    return linuxConfig;
  }

  private static String getSshPath(String adminUsername) {
    return String.format("%s%s%s", "/home/", adminUsername, "/.ssh/authorized_keys");
  }

  /**
   * Deletes a resource group.
   * <p>
   * WARNING: This will delete ALL resources (VM, storage etc) under the resource group.
   * NOTE: Test use only.
   *
   * @param context Azure ResourceContext object, contains the resource group name
   * @throws IOException
   * @throws ServiceException
   */
  public void deleteResourceGroup(ResourceContext context) throws IOException, ServiceException {
    deleteResourceGroup(context.getResourceGroupName());
  }

  /**
   * Deletes a resource group.
   * <p>
   * WARNING: This will delete ALL resources (VM, storage etc) under the resource group.
   * NOTE: Test use only.
   *
   * @param resourceGroup Azure resource group name
   * @throws IOException
   * @throws ServiceException
   */
  public void deleteResourceGroup(String resourceGroup) throws IOException, ServiceException {
    resourceManagementClient.getResourceGroupsOperations().beginDeleting(resourceGroup);
  }

  public AzureComputeInstanceHelper createAzureComputeInstanceHelper(VirtualMachine vm,
    AzureCredentials cred, String resourceGroup) throws IOException, ServiceException {
    // AZURE_SDK Azure SDK requires the following calls to correctly create clients.
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ManagementConfiguration.class.getClassLoader());
    try {
      return new AzureComputeInstanceHelper(vm, cred, resourceGroup);
    } finally {
      Thread.currentThread().setContextClassLoader(contextLoader);
    }
  }

  /**
   * Gets a list of all resource groups under the subscription.
   *
   * @return a list of all resource groups under the subscription
   * @throws ServiceException
   * @throws IOException
   * @throws URISyntaxException
   */
  public ArrayList<ResourceGroupExtended> getResourceGroups() throws ServiceException, IOException,
    URISyntaxException {
    return resourceManagementClient.getResourceGroupsOperations().list(null).getResourceGroups();
  }

  public ResourceGroupExtended getResourceGroup(String resourceGroupName)
    throws ServiceException, IOException, URISyntaxException {
    return resourceManagementClient.getResourceGroupsOperations().get(resourceGroupName)
      .getResourceGroup();
  }

  /**
   * Gets info of all VMs in a resource group.
   *
   * @param resourceGroup name of resource group
   * @return a list of containing information of all VMs in the resource group
   * @throws ServiceException
   * @throws IOException
   * @throws URISyntaxException
   */
  public synchronized ArrayList<VirtualMachine> getVirtualMachines(String resourceGroup)
    throws ServiceException, IOException, URISyntaxException {
    return computeManagementClient.getVirtualMachinesOperations().list(resourceGroup)
      .getVirtualMachines();
  }

  /**
   * Deletes a VM asynchronously. Polling of the operation result is needed to find out if the
   * result of the deletion.
   * <p>
   * NOTE: deleting a VM does not delete the resources (NIC, PublicIP, StorageAccount) it uses.
   *
   * @param resourceGroup name of resource group
   * @param vm            Azure VirtualMachine object, contains info about a particular VM
   * @return response of submitting the delete operation
   * @throws IOException
   * @throws ServiceException
   */
  public synchronized DeleteOperationResponse beginDeleteVirtualMachine(String resourceGroup,
    VirtualMachine vm)
    throws IOException, ServiceException {
    return computeManagementClient.getVirtualMachinesOperations().beginDeleting(
      resourceGroup, vm.getName());
  }

  /**
   * Deletes an AvailabilitySet resource in Azure synchronously.
   *
   * @param resourceGroup name of resource group
   * @param vm            Azure VirtualMachine object, contains info about a particular VM
   * @return response of the delete operation
   * @throws IOException
   * @throws ServiceException
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws URISyntaxException
   */
  public synchronized OperationResponse beginDeleteAvailabilitySetOnVM(String resourceGroup,
    VirtualMachine vm)
    throws IOException, ServiceException, ExecutionException, InterruptedException,
    URISyntaxException {

    String asName = getAvailabilitySetNameFromVm(vm);
    AvailabilitySet as = computeManagementClient.getAvailabilitySetsOperations()
      .get(resourceGroup, asName).getAvailabilitySet();
    if (as.getVirtualMachinesReferences().size() == 0)
      return computeManagementClient.getAvailabilitySetsOperations().delete(resourceGroup,
        getAvailabilitySetNameFromVm(vm));
    else {
      OperationResponse response = new OperationResponse();
      // can't delete while other vm still on the availability set, therefore the operation is
      // forbidden
      response.setStatusCode(HttpStatus.SC_FORBIDDEN);
      return response;
    }
  }

  /**
   * Deletes network resources (NIC & PublicIP) used by a VM synchronously.
   * <p>
   * NOTE: NIC must be deleted first before PublicIP can be deleted.
   *
   * @param resourceGroup        name of resource group
   * @param vm                   Azure VirtualMachine object, contains info about a particular VM
   * @param isPublicIPConfigured does the template define a public IP that should be cleaned up
   * @return response of the delete operations
   * @throws IOException
   * @throws ServiceException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public synchronized OperationResponse beginDeleteNetworkResourcesOnVM(String resourceGroup,
    VirtualMachine vm, boolean isPublicIPConfigured)
    throws IOException, ServiceException, ExecutionException, InterruptedException {
    NetworkInterfaceGetResponse networkInterfaceGetResponse = networkResourceProviderClient
      .getNetworkInterfacesOperations().get(resourceGroup, getNicNameFromVm(vm));
    NetworkInterface nic = networkInterfaceGetResponse.getNetworkInterface();
    NetworkInterfaceIpConfiguration ipConfiguration = nic.getIpConfigurations().get(0);

    // blocking call, deletion of PublicIP need to wait for deletion of NIC to finish
    OperationResponse response = networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(resourceGroup, getNicNameFromVm(vm));

    if (isPublicIPConfigured && ipConfiguration.getPublicIpAddress() != null) {
      String[] pipID = ipConfiguration.getPublicIpAddress().getId().split("/");
      String pipName = pipID[pipID.length - 1];
      response = networkResourceProviderClient.getPublicIpAddressesOperations().beginDeleting(
        resourceGroup, pipName);
      LOG.debug("Begin deleting public IP address {}.", pipName);
    } else {
      LOG.debug("Skipping delete of public IP address: isPublicIPConfigured {}; " +
        "ipConfiguration.getPublicIpAddress(): {}", isPublicIPConfigured,
        ipConfiguration.getPublicIpAddress());
    }
    return response;
  }

  /**
   * Deletes the storage account used by a VM synchronously.
   *
   * @param resourceGroup name of resource group
   * @param vm            Azure VirtualMachine object, contains info about a particular VM
   * @return response of the delete operation
   * @throws IOException
   * @throws ServiceException
   */
  public synchronized OperationResponse beginDeleteStorageAccountOnVM(String resourceGroup,
    VirtualMachine vm) throws IOException, ServiceException {
    return storageManagementClient.getStorageAccountsOperations().delete(resourceGroup,
      getStorageAccountFromVM(vm));
  }

  /**
   * Extracts NIC resource name from VM information.
   *
   * @param vm Azure VirtualMachine object, contains info about a particular VM
   * @return NIC resource name
   */
  public synchronized String getNicNameFromVm(VirtualMachine vm) {
    String text = vm.getNetworkProfile().getNetworkInterfaces().get(0).getReferenceUri();
    Pattern pattern = Pattern.compile(AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
      .getString(Configurations.AZURE_CONFIG_INSTANCE_NIC_FROM_URL_REGEX));
    Matcher matcher = pattern.matcher(text);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Extracts AvailabilitySet resource name from VM information.
   *
   * @param vm Azure VirtualMachine object, contains info about a particular VM
   * @return AvailabilitySet resource name.
   */
  public synchronized String getAvailabilitySetNameFromVm(VirtualMachine vm) {
    String text = vm.getAvailabilitySetReference().getReferenceUri();
    Pattern pattern = Pattern.compile(AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
      .getString(Configurations.AZURE_CONFIG_INSTANCE_AVAILABILITY_SET_FROM_URL_REGEX));
    Matcher matcher = pattern.matcher(text);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Extracts StorageAccount resource name from VM information. The OS VHD URL is assumed to always
   * have the following format:
   *     https://storage_account_name.storage_endpoint_suffix/vhds/vhd_name.vhd
   * storage_endpoint_suffix is different for sovereign clouds (or government regions)
   *
   * @param vm Azure VirtualMachine object, contains info about a particular VM
   * @return StorageAccount resource name
   */
  public synchronized String getStorageAccountFromVM(VirtualMachine vm) {
    String text = vm.getStorageProfile().getOSDisk().getVirtualHardDisk().getUri();
    Pattern pattern = Pattern.compile(AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
      .getString(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_FROM_URL_REGEX));
    Matcher matcher = pattern.matcher(text);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Gets the status of a VM from Azure.
   *
   * @param resourceGroup name of resource group
   * @param vmName        name of VM
   * @return status of the VM
   * @throws ServiceException
   * @throws IOException
   * @throws URISyntaxException
   */
  public synchronized InstanceState getVirtualMachineStatus(String resourceGroup, String vmName)
    throws ServiceException, IOException, URISyntaxException {
    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      resourceGroup, vmName).getVirtualMachine();
    // AZURE_SDK there _seems_ to be 2 InstanceViewStatus: ProvisioningState & PowerState
    ArrayList<InstanceViewStatus> list = vm.getInstanceView().getStatuses();
    for (InstanceViewStatus status : list) {
      LOG.debug("VM {} state is {}.", vmName, status.getCode());
      if (status.getCode().equals(AzureVirtualMachineState.POWER_STATE_RUNNING)) {
        return new SimpleInstanceState(InstanceStatus.RUNNING);
      } else if (status.getCode().equals(AzureVirtualMachineState.POWER_STATE_DEALLOCATED)) {
        return new SimpleInstanceState(InstanceStatus.STOPPED);
      } else if (status.getCode().contains(AzureVirtualMachineState.PROVISIONING_STATE_SUCCEEDED)) {
        return new SimpleInstanceState(InstanceStatus.RUNNING);
      } else if (status.getCode().contains(AzureVirtualMachineState.PROVISIONING_STATE_DELETING)) {
        return new SimpleInstanceState(InstanceStatus.DELETING);
      } else if (status.getCode().contains(AzureVirtualMachineState.PROVISIONING_STATE_FAILED)) {
        return new SimpleInstanceState(InstanceStatus.FAILED);
      } else if (status.getCode().toLowerCase().contains("fail")) {
        // AZURE_SDK Any state that has the word 'fail' in it indicates VM is in FAILED state
        return new SimpleInstanceState(InstanceStatus.FAILED);
      }
      // FIXME find out if Azure VM has start(ing) or delete (deleting) states
    }
    LOG.debug("VM {} state is UNKNOWN. {}", vmName, vm);
    return new SimpleInstanceState(InstanceStatus.UNKNOWN);
  }

  public synchronized VirtualNetwork getVirtualNetworkByName(String resourceGroup, String vnetName)
    throws IOException, ServiceException {
    return networkResourceProviderClient.getVirtualNetworksOperations().get(resourceGroup, vnetName)
      .getVirtualNetwork();
  }

  public synchronized NetworkSecurityGroup getNetworkSecurityGroupByName(String resourceGroup,
    String networkSecurityGroupName) throws IOException, ServiceException {
    return networkResourceProviderClient.getNetworkSecurityGroupsOperations().get(resourceGroup,
      networkSecurityGroupName).getNetworkSecurityGroup();
  }

  public synchronized AvailabilitySet getAvailabilitySetByName(String resourceGroup,
    String availabilitySetName)
    throws IOException, ServiceException, URISyntaxException {
    return computeManagementClient.getAvailabilitySetsOperations().get(
      resourceGroup, availabilitySetName).getAvailabilitySet();
  }

  /**
   * Grab the list of Azure VM sizes that can be deployed into the Availability Set. This list
   * depends on what is already in the AS and can change as VMs are added or deleted. I.e. since
   * different versions of the same VM size (e.g. v2 vs. non-v2) cannot coexist in the same AS all
   * VMs of a specific size need to be deleted from the AS before VMs of an incompatible size can be
   * added.
   *
   * @param resourceGroupName   name of resource group
   * @param availabilitySetName name of availability set
   * @return                    list of VM sizes that can be deployed into the availability set
   * @throws IOException        when there is a network communication error
   * @throws ServiceException   when resource group and/or availability set does not exist
   */
  public synchronized VirtualMachineSizeListResponse getAvailableSizesInAS(String resourceGroupName,
    String availabilitySetName) throws IOException, ServiceException {
    return computeManagementClient.getAvailabilitySetsOperations()
      .listAvailableSizes(resourceGroupName, availabilitySetName);
  }

  /**
   * Gets the subnet resource under a VNET resource using the name of the subnet resource.
   *
   * @param vnetRgName  Resource Group the VNET is in
   * @param vnetName    Name of the VNET resource
   * @param subnetName  Name of the subnet resource
   * @return Subnet resource under the VNET
   * @throws IOException
   * @throws ServiceException
   */
  public synchronized Subnet getSubnetByName(
    String vnetRgName, String vnetName, String subnetName) throws IOException, ServiceException {
    return networkResourceProviderClient.getSubnetsOperations().get(
      vnetRgName, vnetName, subnetName).getSubnet();
  }

  /**
   * Deletes a StorageAccount resource synchronously.
   *
   * @param resourceGroup      name of resource group
   * @param storageAccountName name of StorageAccount resource
   * @return response of the delete operation
   * @throws IOException
   * @throws ServiceException
   */
  public synchronized OperationResponse beginDeleteStorageAccountByName(String resourceGroup,
    String storageAccountName) throws IOException, ServiceException {
    return storageManagementClient.getStorageAccountsOperations().delete(resourceGroup,
      storageAccountName);
  }

  /**
   * Deletes a AvailabilitySet resource synchronously.
   *
   * @param resourceGroup       name of resource group
   * @param availabilitySetName name of AvailabilitySet resource
   * @return response of the delete operation
   * @throws ServiceException
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  public synchronized OperationResponse beginDeleteAvailabilitySetByName(String resourceGroup,
    String availabilitySetName)
    throws ServiceException, ExecutionException, InterruptedException, IOException {
    return computeManagementClient.getAvailabilitySetsOperations().delete(resourceGroup,
      availabilitySetName);
  }

  /**
   * Deletes a NetworkInterface resource synchronously.
   *
   * @param resourceGroup        name of resource group
   * @param networkInterfaceName name of NetworkInterface resource
   * @return response of the delete operation
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  public synchronized OperationResponse beginDeleteNetworkInterfaceByName(String resourceGroup,
    String networkInterfaceName) throws InterruptedException, ExecutionException, IOException {
    return networkResourceProviderClient.getNetworkInterfacesOperations().delete(resourceGroup,
      networkInterfaceName);
  }

  /**
   * Deletes a PublicIP resource synchronously.
   *
   * @param resourceGroup       name of resource group
   * @param publicIpAddressName name of PublicIP resource
   * @return response of the delete operation
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  public synchronized OperationResponse beginDeletePublicIpAddressByName(String resourceGroup,
    String publicIpAddressName) throws InterruptedException, ExecutionException, IOException {
    return networkResourceProviderClient.getPublicIpAddressesOperations().delete(resourceGroup,
      publicIpAddressName);
  }

  /**
   * Builds a ResourceContext object from Azure SDK VirtualMachine object.
   *
   * @param resourceGroupName resource group name
   * @param vm                Azure VirtualMachine object, contains info about a particular VM
   * @return a ResourceContext object populated with VM's info
   */
  public synchronized ResourceContext getResourceContextFromVm(String resourceGroupName,
    VirtualMachine vm)
    throws TransientProviderException {
    ResourceContext ctx = new ResourceContext(vm.getLocation(), resourceGroupName,
      resourceManagementClient.getCredentials().getSubscriptionId());
    ctx.setVMInput(vm);
    return ctx;
  }
}
