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
 * limitations under the License.
 */

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.MANAGED_OS_DISK_SUFFIX;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getVmName;

import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.StorageProfile;
import com.cloudera.director.spi.v2.model.InstanceTemplate;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class AzureComputeProviderLiveTestHelper {

  /**
   * Gets all resources associated with a list of instance ids and returns true if none of those resources exist in
   * Azure; false otherwise.
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param map the template in map form used to get configs
   * @param instanceIds the ids to check
   * @return whether or not all resources associated with the ids don't exist in Azure.
   * @throws Exception
   */
  static boolean resourcesDeleted(Azure azure,
      Map<String, String> map,
      Collection<String> instanceIds)
      throws Exception {
    String rgName =
        map.get(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap().getConfigKey());
    String prefix = map
        .get(InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX.unwrap().getConfigKey());
    boolean managed =
        map.get(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey()).equals("Yes");
    int numberOfDataDisks = Integer.parseInt(
        map.get(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap().getConfigKey()));

    return resourcesDeleted(azure, instanceIds, rgName, prefix, managed, numberOfDataDisks);
  }

  /**
   * Gets all resources associated with a list of instance ids and returns true if none of those resources exist in
   * Azure; false otherwise.
   *
   * @param azure the entry point for accessing resource management APIs in Azure
   * @param provider the Azure provider
   * @param template the template used to get configs
   * @param instanceIds the ids to check
   * @return whether or not all resources associated with the ids don't exist in Azure.
   * @throws Exception
   */
  static boolean resourcesDeleted(Azure azure,
      AzureComputeProvider provider,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws Exception {
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(provider.getLocalizationContext());
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);
    String prefix = template.getConfigurationValue(
        InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX,
        templateLocalizationContext);
    boolean managed = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS,
        templateLocalizationContext).equals("Yes");
    int numberOfDataDisks = Integer.parseInt(template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT,
        templateLocalizationContext));

    return resourcesDeleted(azure, instanceIds, rgName, prefix, managed, numberOfDataDisks);
  }

  /**
   * Extracts StorageAccount resource name from storage profile.
   *
   * @param storageProfile storage profile of a virtual machine
   * @return StorageAccount resource name
   */
  static String getStorageAccountNameFromStorageProfile(StorageProfile storageProfile) {
    String text = storageProfile.osDisk().vhd().uri();
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
   * Does the heavy lifting for the other resourcesDeleted methods.
   */
  private static boolean resourcesDeleted(
      Azure azure,
      Collection<String> instanceIds,
      String rgName,
      String prefix,
      boolean managed,
      int numberOfDataDisks)
      throws Exception {
    for (String id : instanceIds) {
      String commonResourceNamePrefix = getFirstGroupOfUuid(id);

      // VM
      if (azure.virtualMachines().getByResourceGroup(rgName, getVmName(id, prefix)) != null) {
        return false;
      }

      // storage
      if (managed) {
        // OS disk
        if (azure.disks().getByResourceGroup(
            rgName, commonResourceNamePrefix + MANAGED_OS_DISK_SUFFIX) != null) {
          return false;
        }
        // data disks
        for (int i = 0; i < numberOfDataDisks; i += 1) {
          if (azure.disks().getByResourceGroup(rgName, commonResourceNamePrefix + "-" + i) != null) {
            return false;
          }
        }
      } else {
        if (azure.storageAccounts().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
          return false;
        }
      }

      // nic
      if (azure.networkInterfaces().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
        return false;
      }

      // pip
      if (azure.publicIPAddresses().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
        return false;
      }
    }

    return true;
  }
}
