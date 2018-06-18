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

import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.Disk;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.network.NetworkInterface;
import com.microsoft.azure.management.network.PublicIPAddress;
import com.microsoft.azure.management.storage.StorageAccount;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulation for the various Azure Resource Ids associated with a Virtual Machine / Instance Id.
 *
 * Also includes static helper methods for dealing with Lists of AzureVirtualMachineMetadata objects.
 */
public class AzureVirtualMachineMetadata {
  //
  // The Azure Resource Ids
  //
  private String vmId;
  private List<String> mdIds = new ArrayList<>();
  private String saId;
  private String nicId;
  private String pipId;

  private String instanceId;
  private boolean useManagedDisks = true; // default
  private boolean hasPublicIp;

  /**
   * Populates the Azure Resource Ids associated with an instance id by querying Azure.
   *
   * Resources associated with a give instance id:
   *   - Virtual Machine
   *   - Managed Disks (or Storage Account)
   *   - Network Interface
   *   - Public IP (if applicable)
   *
   * @param azure the entry point object for accessing resource management APIs in Azure
   * @param instanceId the instance identifier
   * @param template the template used to get fields
   * @param localizationContext the localization context for the template
   */
  AzureVirtualMachineMetadata(Azure azure, String instanceId, AzureComputeInstanceTemplate template,
      LocalizationContext localizationContext) {
    this.instanceId = instanceId;
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext));
    String prefix = template.getInstanceNamePrefix();
    useManagedDisks = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext))
        .equals("Yes");
    int numberOfManagedDisks = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext)));
    hasPublicIp = template.getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP,
        SimpleResourceTemplate.getTemplateLocalizationContext(localizationContext)).equals("Yes");
    String commonResourceNamePrefix = AzureComputeProvider.getFirstGroupOfUuid(instanceId);

    // Virtual Machines
    VirtualMachine vm =
        azure.virtualMachines().getByResourceGroup(rgName, AzureComputeProvider.getVmName(instanceId, prefix));
    if (vm != null) {
      vmId = vm.id();
    }

    // Storage
    if (useManagedDisks) {
      // Managed Disks
      // OS
      Disk osd = azure.disks()
          .getByResourceGroup(rgName, commonResourceNamePrefix + AzureComputeProvider.MANAGED_OS_DISK_SUFFIX);
      if (osd != null) {
        mdIds.add(osd.id());
      }
      for (int i = 0; i < numberOfManagedDisks; i += 1) {
        // data disks
        Disk dd = azure.disks().getByResourceGroup(rgName, commonResourceNamePrefix + "-" + i);
        if (dd != null) {
          mdIds.add(dd.id());
        }
      }
    } else {
      // Storage Account
      StorageAccount sa = azure.storageAccounts().getByResourceGroup(rgName, commonResourceNamePrefix);
      if (sa != null) {
        saId = sa.id();
      } else if (vm != null) {
        // Director <= v2.5 used a different naming scheme for Storage Accounts which can't be inferred by the UUID
        String saName = AzureComputeProvider.getStorageAccountNameFromVM(vm);
        sa = azure.storageAccounts().getByResourceGroup(rgName, saName);
        if (sa != null) {
          saId = sa.id();
        }
      }
    }

    // Network Interface
    NetworkInterface ni = azure.networkInterfaces().getByResourceGroup(rgName, commonResourceNamePrefix);
    if (ni != null) {
      nicId = ni.id();
    } else if (vm != null) {
      // Director <= v2.5 used a different naming scheme for Network Interfaces which can't be inferred by the UUID
      nicId = vm.primaryNetworkInterfaceId();
    }

    // Public IP
    if (hasPublicIp) {
      PublicIPAddress pip = azure.publicIPAddresses().getByResourceGroup(rgName, commonResourceNamePrefix);
      if (pip != null) {
        pipId = pip.id();
      } else if (vm != null) {
        // Director <= v2.5 used a different naming scheme for Public IPs which can't be inferred by the UUID
        pipId = vm.getPrimaryPublicIPAddressId();
      }
    }
  }

  /**
   * Checks if any resources still exist in Azure.
   *
   * @return true if resources still exist; false otherwise
   */
  boolean resourcesExist() {
    return !(isEmpty(vmId) && mdIds.isEmpty() && isEmpty(saId) && isEmpty(nicId) && isEmpty(pipId));
  }

  /**
   * Pretty print the Resource Ids found in Azure.
   *
   * @return the formatted String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    if (resourcesExist()) {
      sb.append(String.format("The following Azure Resource Ids associated with instance id %s exist in Azure.",
          instanceId));

      if (!isEmpty(vmId)) {
        sb.append(String.format(" Virtual Machine: %s;", vmId));
      }

      if (useManagedDisks && !mdIds.isEmpty()) {
        sb.append(String.format(" Managed Disks: %s;", mdIds));
      } else if (!useManagedDisks && !isEmpty(saId)) {
        sb.append(String.format(" Storage Account: %s;", saId));
      }

      if (!isEmpty(nicId)) {
        sb.append(String.format(" Network Interface: %s;", nicId));
      }

      if (hasPublicIp && !isEmpty(pipId)) {
        sb.append(String.format(" Public IP: %s;", pipId));
      }

      // replace the last semicolon with a period
      sb.setLength(sb.length() - 1);
      sb.append(".");
    } else {
      sb.append(String.format("No Azure Resources (Virtual Machine, Storage, Network Interface, or Public IP) " +
          "associated with instance id %s exist in Azure.", instanceId));
    }

    return sb.toString();
  }

  /**
   * Returns a new list of just the Virtual Machine Resource Ids pulled out of a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the new list of VM Resource Ids
   */
  static List<String> getVmIds(List<AzureVirtualMachineMetadata> metadatas) {
    List <String> ids = new ArrayList<>();
    for (AzureVirtualMachineMetadata i : metadatas) {
      if (!isEmpty(i.vmId)) {
        ids.add(i.vmId);
      }
    }
    return ids;
  }

  /**
   * Returns a new list of just the Managed Disk Resource Ids pulled out of a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the new list of MD Resource Ids
   */
  static List<String> getMdIds(List<AzureVirtualMachineMetadata> metadatas) {
    List <String> ids = new ArrayList<>();
    for (AzureVirtualMachineMetadata i : metadatas) {
      ids.addAll(i.mdIds);
    }
    return ids;
  }

  /**
   * Returns a new list of just the Storage Account Resource Ids pulled out of a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the new list of SA Resource Ids
   */
  static List<String> getSaIds(List<AzureVirtualMachineMetadata> metadatas) {
    List <String> ids = new ArrayList<>();
    for (AzureVirtualMachineMetadata i : metadatas) {
      if (!isEmpty(i.saId)) {
        ids.add(i.saId);
      }
    }
    return ids;
  }

  /**
   * Returns a new list of just the Network Interface Resource Ids pulled out of a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the new list of Nic Resource Ids
   */
  static List<String> getNicIds(List<AzureVirtualMachineMetadata> metadatas) {
    List <String> ids = new ArrayList<>();
    for (AzureVirtualMachineMetadata i : metadatas) {
      if (!isEmpty(i.nicId)) {
        ids.add(i.nicId);
      }
    }
    return ids;
  }

  /**
   * Returns a new list of just the Public IP Resource Ids pulled out of a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the new list of Public IP Resource Ids
   */
  static List<String> getpipIds(List<AzureVirtualMachineMetadata> metadatas) {
    List <String> ids = new ArrayList<>();
    for (AzureVirtualMachineMetadata i : metadatas) {
      if (!isEmpty(i.pipId)) {
        ids.add(i.pipId);
      }
    }
    return ids;
  }

  /**
   * Pretty print the Resource Ids found in Azure for a list of metadata objects.
   *
   * @param metadatas the list of metadata to use
   * @return the formatted String
   */
  static String metadataListToString(List<AzureVirtualMachineMetadata> metadatas) {
    // log all the Resource Ids
    StringBuilder sb = new StringBuilder();
    for (AzureVirtualMachineMetadata i : metadatas) {
      sb.append(i.toString());
    }
    return sb.toString();
  }
}
