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

import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.microsoft.azure.management.compute.models.AvailabilitySetReference;
import com.microsoft.azure.management.compute.models.CachingTypes;
import com.microsoft.azure.management.compute.models.ComputeOperationResponse;
import com.microsoft.azure.management.compute.models.DiskCreateOptionTypes;
import com.microsoft.azure.management.compute.models.HardwareProfile;
import com.microsoft.azure.management.compute.models.ImageReference;
import com.microsoft.azure.management.compute.models.NetworkInterfaceReference;
import com.microsoft.azure.management.compute.models.NetworkProfile;
import com.microsoft.azure.management.compute.models.OSDisk;
import com.microsoft.azure.management.compute.models.OSProfile;
import com.microsoft.azure.management.compute.models.Plan;
import com.microsoft.azure.management.compute.models.PurchasePlan;
import com.microsoft.azure.management.compute.models.StorageProfile;
import com.microsoft.azure.management.compute.models.VirtualHardDisk;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.compute.models.VirtualMachineImage;
import com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.microsoft.azure.management.network.models.Subnet;
import com.microsoft.azure.management.network.models.VirtualNetwork;
import com.microsoft.azure.utility.ComputeHelper;
import com.microsoft.azure.utility.ResourceContext;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to create a VM.
 *
 * If there is any failure, the context used to create VM is returned as part of the task result.
 */
public class CreateVMTask extends AbstractAzureComputeProviderTask implements Callable<TaskResult> {
  private ResourceContext context;
  // VM resources
  private VirtualNetwork vnet;
  private NetworkSecurityGroup nsg;
  private AvailabilitySet as;
  private String vmSize;
  private String vmName;
  private String vmNamePrefix;
  private String instanceId;
  private String fqdnSuffix;
  private String adminName;
  private String sshPublicKey;
  private Subnet subnet;
  private int dataDiskCount;
  private AzureVmImageInfo imageInfo;

  private static final Logger LOG = LoggerFactory.getLogger(CreateVMTask.class);
  private DateTime startTime;

  // N.B.: `vmName` is composed of the user defined vm name prefix from the director template
  // and the instance id (a UUID) supplied by director
  public CreateVMTask(ResourceContext context, VirtualNetwork vnet, Subnet subnet,
    NetworkSecurityGroup nsg, AvailabilitySet as, String vmSize, String vmNamePrefix,
    String instanceId, String fqdnSuffix, String adminName, String sshPublicKey, int dataDiskCount,
    AzureVmImageInfo imageInfo, AzureComputeProviderHelper computeProviderHelper) {
    this.context = context;
    this.vnet = vnet;
    this.nsg = nsg;
    this.as = as;
    this.vmSize = vmSize;
    this.vmName = vmNamePrefix + "-" + instanceId;
    this.vmNamePrefix = vmNamePrefix;
    this.instanceId = instanceId;
    this.fqdnSuffix = fqdnSuffix;
    this.adminName = adminName;
    this.sshPublicKey = sshPublicKey;
    this.dataDiskCount = dataDiskCount;
    this.imageInfo = imageInfo;
    this.computeProviderHelper = computeProviderHelper;
    this.startTime = DateTime.now();
    this.subnet = subnet;
  }

  /**
   * Request and prepare VM resources.
   *
   * NOTE: If any resource request has failed, this method (Azure SDK calls) will throw exception
   * and the CreateVMTask will fail. Cleanup of the resources is handled by the task submitter
   * using the ResourceContext in TaskResult.
   *
   * @throws Exception several Azure SDK API calls throws generic exceptions
   */
  private void requestResources() throws Exception {
    // AZURE_SDK Azure SDK API StorageHelper#createStorageAccount throws generic Exception
    computeProviderHelper.createAndSetStorageAccount(context);
    LOG.info("Created StorageAccount: {}, for VM {}.", context.getStorageAccountName(), vmName);


    // AZURE_SDK Azure SDK API NetworkHelper throws generic Exception
    context.setVirtualNetwork(vnet);
    computeProviderHelper.createAndSetNetworkInterface(context, subnet);
    LOG.info("Created NetworkInterface: {}, for VM {}.", context.getNetworkInterfaceName(), vmName);

    // AZURE_SDK Currently Create NSG will fail due to bug in Azure SDK.
    // Update NIC to use existing NSG.
    computeProviderHelper.setNetworkSecurityGroup(nsg, context);
    LOG.info("Use existing NetworkSecurityGroup: {}, for VM {}.", nsg.getName(), vmName);

    String vhdContainer = ComputeHelper.getVhdContainerUrl(context);
    String osVhduri = vhdContainer + String.format("/os%s.vhd", "osvhd");

    VirtualMachine vm = new VirtualMachine(context.getLocation());
    vm.setName(vmName);

    // Set tags
    if (context.getTags() != null) {
      vm.setTags(context.getTags());
    }

    vm.setType("Microsoft.Compute/virtualMachines");

    // Set availability set
    AvailabilitySetReference asRef = new AvailabilitySetReference();
    asRef.setReferenceUri(as.getId());
    vm.setAvailabilitySetReference(asRef);
    LOG.info("Use existing AvailabilitySet: {}, for VM {}.", as.getName(), vmName);

    //set hardware profile
    HardwareProfile hwProfile = new HardwareProfile();
    hwProfile.setVirtualMachineSize(vmSize);
    vm.setHardwareProfile(hwProfile);

    String publisher = imageInfo.getPublisher();
    String sku = imageInfo.getSku();
    String offer = imageInfo.getOffer();
    String version = imageInfo.getVersion();

    // Set storage profile
    StorageProfile sto = new StorageProfile();
    VirtualMachineImage vmimage = computeProviderHelper.getMarketplaceVMImage(
      context.getLocation(), imageInfo);
    ImageReference ir = new ImageReference();
    ir.setPublisher(publisher);
    ir.setOffer(offer);
    ir.setSku(sku);
    ir.setVersion(version);
    sto.setImageReference(ir);
    // This is a thread safe call as inputs are all local to this task
    PurchasePlan purchasePlan = computeProviderHelper.getPurchasePlan(vmimage);

    // Set purchase plan if the image has one attached. Certain images does not have purchase plan
    // attached.
    if (purchasePlan != null) {
      Plan plan = new Plan();
      plan.setName(purchasePlan.getName());
      plan.setProduct(purchasePlan.getProduct());
      plan.setPromotionCode(null);
      plan.setPublisher(purchasePlan.getPublisher());
      vm.setPlan(plan);
    } else {
      LOG.info("Image {} does not have purchase plan attached.", imageInfo);
    }

    // Setup storage, osdisk + datadisk
    VirtualHardDisk vhardDisk = new VirtualHardDisk();
    vhardDisk.setUri(osVhduri);
    OSDisk osDisk = new OSDisk("osdisk", vhardDisk, DiskCreateOptionTypes.FROMIMAGE);
    osDisk.setCaching(CachingTypes.NONE);
    sto.setOSDisk(osDisk);
    // This is a thread safe call as inputs are all local to this task
    sto.setDataDisks(computeProviderHelper.createDataDisks(
      dataDiskCount, AzureComputeProviderHelper.SIZE_IN_GB, vhdContainer));

    vm.setStorageProfile(sto);

    // Set network profile
    NetworkProfile networkProfile = new NetworkProfile();
    NetworkInterfaceReference nir = new NetworkInterfaceReference();
    nir.setReferenceUri(context.getNetworkInterface().getId());
    ArrayList<NetworkInterfaceReference> nirs = new ArrayList<>(1);
    nirs.add(nir);
    networkProfile.setNetworkInterfaces(nirs);
    vm.setNetworkProfile(networkProfile);

    String vmShortName = AzureComputeProviderHelper.getShortVMName(vmNamePrefix, instanceId);

    // Set os profile
    OSProfile osProfile = new OSProfile();
    osProfile.setAdminUsername(adminName);
    // This is a thread safe call as inputs are all local to this task
    osProfile.setComputerName(vmShortName + "." + fqdnSuffix);
    osProfile.setLinuxConfiguration(
      AzureComputeProviderHelper.createSshLinuxConfig(osProfile, sshPublicKey));
    vm.setOSProfile(osProfile);
    vm.setTags(context.getTags());
    context.setVMInput(vm);

    // set DNS label, forward & backward FQDN
    if (context.isCreatePublicIpAddress()) {
      computeProviderHelper.setPublicDNSInfo(context, vmShortName);
    }
  }

  public TaskResult call() {
    boolean success = false;
    ComputeOperationResponse op;
    int successCount = 0;

    try {
      requestResources();
      op = computeProviderHelper.submitVmCreationOp(context);
    } catch (Exception e) {
      // AZURE_SDK Catch all exception from Azure SDK calls so that we can always cleanup.
      LOG.error("Failed to create VM {} due to:", vmName, e);
      return new TaskResult(false, this.context);
    }

    try {
      String vmName = context.getVMInput().getName();
      successCount = pollPendingOperation(op, defaultTimeoutInSec, defaultSleepIntervalInSec,
        LOG, vmName);
    } catch (InterruptedException e) {
      LOG.info("VM {} creation is interrupted.", vmName, e);
      return new TaskResult(false, this.context);
    }

    if (successCount == 1) {
      LOG.info("VM provisioning succeeded.");

      /* FIXME temporarily disable VM script runner to speed up VM deployment.
      int scriptSuccessCount = pollPendingOperation(computeProviderHelper.createCustomizedScript(context),
          defaultTimeoutInSec, defaultSleepIntervalInSec, LOG);
      if (scriptSuccessCount == 1) {
        LOG.info("Script execution succeeded");
        exitCode = 0;
      }
      */

      success = true;
      long timeSeconds = (DateTime.now().getMillis() - startTime.getMillis()) / 1000;
      LOG.info("Creation of VM {} took {} seconds.", context.getVMInput().getName(), timeSeconds);
    }

    return new TaskResult(success, this.context);
  }
}
