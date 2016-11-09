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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceHelper;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.AzureVirtualMachineState;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.azure.utils.VmCreationParameters;
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
import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.NetworkInterfaceGetResponse;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.microsoft.azure.management.network.models.OperationStatus;
import com.microsoft.azure.management.network.models.PublicIpAddress;
import com.microsoft.azure.management.network.models.PublicIpAddressDnsSettings;
import com.microsoft.azure.management.network.models.PublicIpAddressGetResponse;
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
  private ExecutorService service = Executors.newCachedThreadPool();

  private static final String LATEST = "latest";
  private static final String PUBLIC_URL_POSTFIX = ".cloudapp.azure.com";
  private static final int THREAD_POOL_SHUTDOWN_WAIT_TIME_SECONDS = 30;

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
   * Creates and kicks off a CreateVMTask.
   * <p/>
   * This function makes multiple calls to Azure backend.
   *
   * @param context                      Azure resource context. Stores detailed info of VM
   *                                     supporting resources when the function returns
   * @param parameters                   contains additional parameters used for VM creation
   *                                     that is not in the Azure resource context
   * @param azureOperationPollingTimeout Azure operation polling timeout specified in second
   * @return A future of CreateVMTask
   */
  public Future<TaskResult> submitVmCreationTask(
    ResourceContext context, VmCreationParameters parameters, int azureOperationPollingTimeout) {

    // Create a task to request resources and create VM
    CreateVMTask task = new CreateVMTask(context, parameters, azureOperationPollingTimeout, this);

    return service.submit(task);
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
   * Constructs the VM FQDN.
   * FQDN format: [vmShortName].[regionName].[Azure public URL postfix]
   * <p/>
   * Cloudera requires using FQDN as host names.
   *
   * @param vmShortName a shortened version of the vm name in the following format:
   *                    [user defined vm name prefix]-[first 8 characters of the instance id (UUID)]
   * @param regionName  name of region (e.g. uswest) where the VM resides
   * @return public FQDN of a VM
   * @see <a href="http://www.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_networknames_configure.html" />
   */
  private String getPublicFqdn(String vmShortName, String regionName) {
    return vmShortName + "." + regionName + PUBLIC_URL_POSTFIX;
  }

  /**
   * Sets the public IP DNS label plus both forward & reverse FQDN.
   *
   * @param context     Azure ResourceContext object. It contains old info without DNS settings
   * @param vmShortName a shortened version of the vm name in the following format:
   *                    [user defined vm name prefix]-[first 8 characters of the instance id (UUID)]
   * @throws Exception various Azure backend calls could fail
   */
  public synchronized void setPublicDNSInfo(ResourceContext context, String vmShortName)
    throws IOException, ServiceException, InterruptedException, ExecutionException {
    String vmName = context.getVMInput().getName();

    // Get latest PIP info
    PublicIpAddressGetResponse resp = networkResourceProviderClient.getPublicIpAddressesOperations()
      .get(context.getResourceGroupName(), context.getPublicIpAddress().getName());
    PublicIpAddress pip = resp.getPublicIpAddress();
    if (pip == null) {
      throw new TransientProviderException("Failed to get public IP config for VM: " +
        vmName + ".");
    }

    LOG.debug("Successfully retrieved public IP: {}.", pip.getName());

    PublicIpAddressDnsSettings dnsSettings = pip.getDnsSettings();
    if (dnsSettings == null) {
      LOG.debug("DNS settings is NULL, create a new one.");
      dnsSettings = new PublicIpAddressDnsSettings();
    }

    String fqdn = getPublicFqdn(vmShortName, context.getLocation().toLowerCase());
    LOG.debug("Public DNS FQDN is: {}.", fqdn);

    dnsSettings.setDomainNameLabel(vmShortName);
    dnsSettings.setFqdn(fqdn);
    dnsSettings.setReverseFqdn(fqdn);
    pip.setDnsSettings(dnsSettings);

    // update public IP setting
    AzureAsyncOperationResponse updateResp =
      networkResourceProviderClient.getPublicIpAddressesOperations().createOrUpdate(
        context.getResourceGroupName(), pip.getName(), pip);
    LOG.debug("Update public DNS resp = {}.", updateResp.getStatus());
    if (!updateResp.getStatus().equals(OperationStatus.SUCCEEDED)) {
      throw new UnrecoverableProviderException("Failed to set DNS host name for VM: " +
        vmName + ".");
    }

    LOG.debug("Successfully set public DNS of VM to: {}.", fqdn);
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

  public PurchasePlan getPurchasePlan(VirtualMachineImage vmImage) {
    return vmImage.getPurchasePlan();
  }

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
   * Check if AvailabilitySet is already created. If not create one and set it to context. If so
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
   * Create Public IP (if configured) and NIC.
   *
   * @param context Azure ResourceContext
   * @return NetworkInterface resource created in Azure
   * @throws Exception Azure SDK API calls throw generic exceptions
   */
  public synchronized NetworkInterface createAndSetNetworkInterface(ResourceContext context,
                                                                    Subnet snet)
    throws Exception {
    if (context.isCreatePublicIpAddress() && context.getPublicIpAddress() == null) {
      // AZURE_SDK NetworkHelper.createPublicIpAddress() throw generic Exception.
      PublicIpAddress pip = NetworkHelper.createPublicIpAddress(
        networkResourceProviderClient, context);

      // AZURE_SDK NetworkHelper.createPublicIpAddress does not pickup tags from context.
      if (context.getTags() != null) {
        pip.setTags(context.getTags());
        AzureAsyncOperationResponse resp =
          networkResourceProviderClient.getPublicIpAddressesOperations().createOrUpdate(
            context.getResourceGroupName(), pip.getName(), pip);
        if (resp.getStatusCode() != HttpStatus.SC_OK) {
          throw new TransientProviderException("Failed to add tags to Public IP " + pip.getName() +
            " status code " + resp.getStatusCode() + ".");
        }
        context.setPublicIpAddress(pip);
        LOG.debug("Successfully updated tags for PublicIP {}.", pip.getName());
      }
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
    StorageAccount sa = StorageHelper.createStorageAccount(
      storageManagementClient, context, stoInput);

    // AZURE_SDK StorageHelper.createStorageAccount() does not pick up tags from context
    if (context.getTags() != null) {
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
   * Delete any VM and resource defined in contexts within the resource group.
   * <p/>
   * Blocks until all resources specified in contexts are deletes or if deletion thread is
   * interrupted.
   *
   * @param resourceGroup                Azure resource group name
   * @param contexts                     Azure context populated during VM allocation
   * @param isPublicIPConfigured         was the resource provisioned with a public IP
   * @param azureOperationPollingTimeout Azure operation polling timeout specified in second
   * @throws InterruptedException
   */
  public void deleteResources(String resourceGroup, Collection<ResourceContext> contexts,
    boolean isPublicIPConfigured, int azureOperationPollingTimeout)
    throws InterruptedException {
    Set<CleanUpTask> tasks = new HashSet<>();
    for (ResourceContext context : contexts) {
      tasks.add(new CleanUpTask(resourceGroup, context, this, isPublicIPConfigured,
        azureOperationPollingTimeout));
    }
    service.invokeAll(tasks);
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
   * Get info of all VMs in a resource group.
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

  public Future<TaskResult> submitDeleteVmTask(String resourceGroup, VirtualMachine vm,
    boolean isPublicIPConfigured, int azureOperationPollingTimeout) {
    CleanUpTask toDelete = new CleanUpTask(resourceGroup, vm, this, isPublicIPConfigured,
      azureOperationPollingTimeout);
    return service.submit(toDelete);
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
    String patternString = ".*/providers/Microsoft.Network/networkInterfaces/(.*)";
    Pattern pattern = Pattern.compile(patternString);
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
    String patternString = ".*/providers/Microsoft.Compute/availabilitySets/(.*)";
    Pattern pattern = Pattern.compile(patternString);
    Matcher matcher = pattern.matcher(text);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Poll a single pending task till it is complete or timeout.
   * Used for test only.
   *
   * @param task             pending task to poll for completion
   * @param durationInSecond overall timeout period
   * @param intervalInSecond poll interval
   * @return number of successful task (0 or 1)
   */
  public int pollPendingTask(Future<TaskResult> task, int durationInSecond,
    int intervalInSecond) {
    Set<Future<TaskResult>> operations = new HashSet<>();
    operations.add(task);
    return pollPendingTasks(operations, durationInSecond, intervalInSecond, null);
  }

  /**
   * Poll pending tasks till all tasks are complete or timeout.
   * Azure platform operation can range from minutes to one hour.
   *
   * @param tasks            set of submitted tasks
   * @param durationInSecond overall timeout period
   * @param intervalInSecond poll interval
   * @param failedContexts   set of failed task contexts. This list contains all the contexts of
   *                         submitted tasks. Context of a successful task is removed from this
   *                         set. When this call returns the element in this set are the contexts
   *                         of failed tasks.
   * @return number of successful tasks
   */
  @SuppressWarnings("PMD.CollapsibleIfStatements")
  public int pollPendingTasks(Set<Future<TaskResult>> tasks, int durationInSecond,
    int intervalInSecond, Set<ResourceContext> failedContexts) {
    Set<Future<TaskResult>> responses = new HashSet<>(tasks);
    int succeededCount = 0;
    int timerInMilliSec = durationInSecond * 1000;
    int intervalInMilliSec = intervalInSecond * 1000;

    try {
      while (timerInMilliSec > 0 && responses.size() > 0) {
        Set<Future<TaskResult>> dones = new HashSet<>();
        for (Future<TaskResult> task : responses) {
          try {
            if (task.isDone()) {
              dones.add(task);
              TaskResult tr = task.get();
              if (tr.isSuccessful()) {
                succeededCount++;
                // Remove successful contexts so that what remains are the failed contexts
                if (failedContexts != null) {
                  if (!failedContexts.remove(tr.getContex())) {
                    LOG.error("ResourceContext {} does not exist in the submitted context list.",
                      tr.getContex());
                  }
                }
              }
            }
          } catch (ExecutionException e) {
            LOG.error("Polling of pending tasks encountered an error: ", e);
          }
        }
        responses.removeAll(dones);

        Thread.sleep(intervalInMilliSec);

        timerInMilliSec = timerInMilliSec - intervalInMilliSec;
        LOG.debug("Polling pending tasks: remaining time = " + timerInMilliSec / 1000 +
          " seconds.");
      }
    } catch (InterruptedException e) {
      LOG.error("Polling of pending tasks was interrupted.", e);
      shutdownTaskRunnerService();
    }

    // Terminate all tasks if we timed out.
    if (timerInMilliSec <= 0 && responses.size() > 0) {
      shutdownTaskRunnerService();
    }

    // Always return the succeeded task count and let the caller decide if any resources needs to be
    // cleaned up
    return succeededCount;
  }

  private void shutdownTaskRunnerService() {
    LOG.debug("Shutting down task runner service.");
    service.shutdownNow();
    try {
      boolean terminated = service.awaitTermination(THREAD_POOL_SHUTDOWN_WAIT_TIME_SECONDS,
        TimeUnit.SECONDS);
      if (terminated == false) {
        LOG.error("Thread pool shutdown timeout elapsed before all resources were terminated");
      }
    } catch (InterruptedException e) {
      LOG.error("Shutdown of thread pool was interrupted.", e);
    }
  }

  /**
   * Extracts StorageAccount resource name from VM information.
   *
   * @param vm Azure VirtualMachine object, contains info about a particular VM
   * @return StorageAccount resource name
   */
  public synchronized String getStorageAccountFromVM(VirtualMachine vm) {
    String text = vm.getStorageProfile().getOSDisk().getVirtualHardDisk().getUri();
    String patternString = "https://(.*)\\.blob\\.core\\.windows\\.net/.*";
    Pattern pattern = Pattern.compile(patternString);
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
