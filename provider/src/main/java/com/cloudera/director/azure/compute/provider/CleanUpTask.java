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
import com.microsoft.azure.management.compute.models.ComputeOperationResponse;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.utility.ResourceContext;
import com.microsoft.windowsazure.core.OperationResponse;
import com.microsoft.windowsazure.exception.ServiceException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to cleanup VM and its supporting resources (NIC, public IP, storage account).
 * <p/>
 * There are 2 types of cleanup tasks:
 * 1. VM is successfully created and live.
 * Cleanup task will delete the VM and then delete the VM's supporting resources.
 * 2. VM is not created but its supporting resources are created.
 * Cleanup task will delete the supporting resources.
 */
public class CleanUpTask extends AbstractAzureComputeProviderTask
  implements Callable<TaskResult> {
  private ResourceContext context;
  private String resourceGroup;
  private VirtualMachine vm; // this is not null if we have a live VM
  private boolean isPublicIPConfigured;
  private DateTime startTime;
  private static final Logger LOG = LoggerFactory.getLogger(CleanUpTask.class);
  private int azureOperationPollingTimeout;

  /**
   * Creates a CleanUpTask.
   *
   * NOTE: This method assume the VM is properly created and the VM object contain all the info
   * needed for proper cleanup.
   * @param resourceGroup         Azure Resource Group name
   * @param vm                    VirtualMachine info
   * @param computeProviderHelper
   * @param isPublicIPConfigured  does the template define a public IP that should be cleaned up
   */
  public CleanUpTask(String resourceGroup, VirtualMachine vm,
    AzureComputeProviderHelper computeProviderHelper, boolean isPublicIPConfigured,
    int azureOperationPollingTimeout) {
    this.resourceGroup = resourceGroup;
    this.vm = vm;
    this.computeProviderHelper = computeProviderHelper;
    this.isPublicIPConfigured = isPublicIPConfigured;
    this.startTime = DateTime.now();
    this.azureOperationPollingTimeout = azureOperationPollingTimeout;
  }

  /**
   * Creates a CleanUpTask.
   *
   * NOTE: VM object may or may not be created properly, use info in the resource context to delete
   * any left over resources.
   * @param resourceGroup Azure Resource Group name
   * @param context       ResourceContext info
   * @param computeProviderHelper
   * @param isPublicIPConfigured
   */
  public CleanUpTask(String resourceGroup, ResourceContext context,
    AzureComputeProviderHelper computeProviderHelper, boolean isPublicIPConfigured,
    int azureOperationPollingTimeout) {
    this.resourceGroup = resourceGroup;
    this.context = context;
    this.computeProviderHelper = computeProviderHelper;
    this.isPublicIPConfigured = isPublicIPConfigured;
    this.startTime = DateTime.now();
    this.vm = null;
    this.azureOperationPollingTimeout = azureOperationPollingTimeout;
  }

  public TaskResult call() {
    // Got a live VM, try delete it and then its supporting resources.
    if (vm != null) {
      return deleteUsingVmInfo();
    }
    // We got context delete using info in the context.
    // This is a MUST in the case where VM didn't create successfully,
    // this will clean up the other resources.
    if (context != null) {
      return deleteUsingContextInfo();
    }

    // Programming error.
    throw new RuntimeException("Programming Error in CleanUpTask.");
  }

  /**
   * Deletes a VM first and then its supporting resources using VirtualMachine info.
   *
   * @return result of CleanUpTask
   */
  private TaskResult deleteUsingVmInfo() {
    if (vm == null) {
      LOG.error("VM identifier is null, can not delete VM and associated resources.");
      return new TaskResult(false, null);
    }

    // Create ResourceContext to hold VM information, verify the required resource info is present.
    ResourceContext ctx = computeProviderHelper.getResourceContextFromVm(resourceGroup, vm);
    try {
      ctx.setNetworkInterfaceName(computeProviderHelper.getNicNameFromVm(vm));
      ctx.setStorageAccountName(computeProviderHelper.getStorageAccountFromVM(vm));
      ctx.setAvailabilitySetName(computeProviderHelper.getAvailabilitySetNameFromVm(vm));
    } catch (Exception e) {
      LOG.error("VirtualMachine info for VM{} is malformed.", vm.getName(), e);
      return new TaskResult(false, ctx);
    }

    String vmName = vm.getName();
    try {
      // Azure SDK throws ServiceException if VM does not exist in the RG
      computeProviderHelper.getVirtualMachineStatus(resourceGroup, vmName);
    } catch (ServiceException | IOException | URISyntaxException e) {
      LOG.debug("Failed to find VM: {} in resource group: {} due to error, can't delete it.",
        vmName, resourceGroup, e);
      // local context delete will/should take care of the rest
      return new TaskResult(false, ctx);
    }

    LOG.info("Begin deleting VM: {} in resource group: {}.", vm.getName(), resourceGroup);
    int successCount = 0;
    try {
      ComputeOperationResponse response =
        computeProviderHelper.beginDeleteVirtualMachine(resourceGroup, vm);
      successCount = pollPendingOperation(response, azureOperationPollingTimeout,
        defaultSleepIntervalInSec, LOG, vm.getName());
    } catch (IOException | ServiceException e) {
      LOG.error("Failed to delete VM {}. Please check Azure portal for any not deleted resources.",
          vm.getName(), e);
    } catch (InterruptedException e) {
      LOG.error("Deletion of VM {} is interrupted. Please check Azure portal for any remaining resources.",
          vm.getName(), e);
    }

    if (successCount != 1) {
      return new TaskResult(false, ctx);
    }

    // Delete the rest of the resources in any order should be fine
    OperationResponse result;
    String nicName = computeProviderHelper.getNicNameFromVm(vm);
    try {
      result = computeProviderHelper.beginDeleteNetworkResourcesOnVM(resourceGroup, vm,
        isPublicIPConfigured);
      LOG.debug("VM: {}. Delete NetworkInterface {} status code: {}.",
        vmName, nicName, result.getStatusCode());
    } catch (IOException | ServiceException | ExecutionException | InterruptedException e) {
      LOG.error("VM: {}. Delete NetworkInterface {} encountered error:", vmName, nicName, e);
    }

    String saName = computeProviderHelper.getStorageAccountFromVM(vm);
    try {
      result = computeProviderHelper.beginDeleteStorageAccountOnVM(resourceGroup, vm);
      LOG.debug("VM: {}. Delete StorageAccount {} status code: {}.",
        vmName, saName, result.getStatusCode());
    } catch (IOException | ServiceException e) {
      LOG.error("VM: {}. Delete StorageAccount {} encountered error:", vmName, saName, e);
    }

    long timeSeconds = (DateTime.now().getMillis() - startTime.getMillis()) / 1000;
    LOG.info("Delete VM {} and its resources took {} seconds.", vm.getName(), timeSeconds);

    return new TaskResult(true, ctx);
  }

  /**
   * Delete VM first and then its supporting resources using ResourceContext info.
   *
   * VM may not have been allocated successfully in this case.
   *
   * @return result of CleanUpTask
   */
  private TaskResult deleteUsingContextInfo() {
    boolean hasError = false;
    String vmName = "UNKNOWN_VM";

    LOG.debug("Begin clean up with ResourceContext used in VM creation");
    if (context.getVMInput() != null) {
      // Context contains VirtualMachine info, try using it to delete first.
      this.vm = context.getVMInput();
      vmName = vm.getName();
      TaskResult deleteVmResult = deleteUsingVmInfo();

      // VM and its supporting resources deleted, all done.
      if (deleteVmResult.isSuccessful()) {
        LOG.info("VM {} and its resources have been cleaned up.", vmName);
        return deleteVmResult;
      }

      // Fall through and cleanup VM resources (NIC, Public IP, Storage Acct etc.)
    }

    OperationResponse result;
    // clean up individual resources if the above steps failed
    if (context.getStorageAccountName() != null) {
      String saName = context.getStorageAccountName();
      try {
        result = computeProviderHelper.beginDeleteStorageAccountByName(resourceGroup, saName);
        LOG.debug("VM: {}. Delete StorageAccount {} status code: {}.",
          vmName, saName, result.getStatusCode());
      } catch (IOException | ServiceException e) {
        hasError = true;
        LOG.error("VM: {}. Delete StorageAccount {} failed:", vmName, saName, e);
      }
    }

    if (context.getNetworkInterface() != null) {
      String nicName = context.getNetworkInterfaceName();
      try {
        result = computeProviderHelper.beginDeleteNetworkInterfaceByName(resourceGroup, nicName);
        LOG.debug("VM: {}. Delete NetworkInterface {} status code: {}.",
          vmName, nicName, result.getStatusCode());
      } catch (IOException | ExecutionException | InterruptedException e) {
        hasError = true;
        LOG.error("VM: {}. Delete NetworkInterface {} encountered error:", vmName, nicName, e);
      }
    }

    if (isPublicIPConfigured && context.getPublicIpAddress() != null) {
      String pip = context.getPublicIpName();
      try {
        result = computeProviderHelper.beginDeletePublicIpAddressByName(resourceGroup, pip);
        LOG.debug("VM: {}. Delete PublicIP {} status code: {}.",
          vmName, pip, result.getStatusCode());
      } catch (InterruptedException | ExecutionException | IOException e) {
        hasError = true;
        LOG.error("VM: {}. Delete PublicIP {} encountered error:", vmName, pip, e);
      }
    } else {
      LOG.debug("Skipping delete of public IP address: isPublicIPConfigured {}; " +
        "context.getPublicIpAddress(): {}", isPublicIPConfigured, context.getPublicIpAddress());
  }

    long timeSeconds = (DateTime.now().getMillis() - startTime.getMillis()) / 1000;
    LOG.info("Delete VM {} context resources took {} seconds.", vm.getName(), timeSeconds);

    return new TaskResult(!hasError, this.context);
  }
}
