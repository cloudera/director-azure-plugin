/*
 *
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
 *
 */

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.Disk;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.PublicIPAddress;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.provider.Launcher;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper test that is used to clean up orphaned resources in a resource group. Orphaned resources
 * are Managed Disks, NICs, PublicIPs that are not attached to a VM.
 *
 * Can also be used to delete VM/VMSS and its relevant resources with a specific tag (key/value pair).
 *
 * NOTE:
 * - This test does not detect/delete orphaned storage accounts.
 * - This test does not detect stale VMs that's still running.
 * - This test WILL delete VMs in "Failed" provision state.
 *
 * To run the test, do:
 * mvn -Dtest.azure.live=true \
 * -Dtest=AzureComputeProviderDeleteOrphanedResourcesTest \
 * -Dtest.azure.orphanedResourceCleanup=true \
 * -Dtest.azure.sshPublicKeyPath=/KeyPath \
 * -Dtest.azure.sshPrivateKeyPath=/KeyPath \
 * -Dazure.live.region=RegionName \
 * -Dazure.live.rg=ResourceGroupName \
 * -DsubscriptionId=subscriptionId \
 * -DtenantId=tenantId \
 * -DclientId=clientId \
 * -DclientSecret=clientSecret \
 * test
 *
 * To specify a tag name and value for vm to be deleted, include options:
 * -Dtest.azure.resourceCleanupVmTagName=TagName
 * -Dtest.azure.resourceCleanupVmTagValue=TagValue
 */

public class AzureComputeProviderDeleteOrphanedResourcesTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AzureComputeProviderDeleteOrphanedResourcesTest.class);

  @BeforeClass
  public static void createLiveTestResources() {
    Assume.assumeTrue(TestHelper.runLiveTests());
    Assume.assumeTrue(TestHelper.runOrphanedResourceCleanup());

    // initialize everything only if live check passes
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null);
  }

  /**
   * Driver test to clean up orphaned resources
   */
  @Test
  public void mainTest() {
    LOG.info("orphanedResourceCleanupUtil");

    String resourceGroupName = TestHelper.TEST_RESOURCE_GROUP;

    AzureCredentials credentials = TestHelper.getAzureCredentials();
    Azure azure = credentials.authenticate();

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null);

    String tagName = TestHelper.resourceCleanupVmTagName();
    String tagValue = TestHelper.resourceCleanupVmTagValue();

    cleanUpVirtualMachines(azure, credentials, resourceGroupName, tagName, tagValue);
    cleanUpVirtualMachineScaleSets(azure, credentials, resourceGroupName, tagName, tagValue);
  }

  private static void cleanUpVirtualMachines(
      Azure azure,
      AzureCredentials credentials,
      String resourceGroupName,
      String tagName,
      String tagValue) {

    VirtualMachineAllocator allocator = new VirtualMachineAllocator(
        azure,
        credentials.getMsiManager(),
        (property, context) -> "");

    ArrayList<String> resourceIds = new ArrayList<>();
    ArrayList<String> vmIds = new ArrayList<>();

    StringBuilder deleteFailedBuilder = new StringBuilder(
        String.format("Delete Failure - not all resources deleted: "));


    // List all VMs in the RG, delete failed one and ones with specific tag/value pair
    // NOTE: we only delete failed VMs but not old VMs. There may be some long-lived VM in the RG
    // we have to keep around (i.e. the DNS server).
    LOG.info("VMs in the resource group: ");
    for (VirtualMachine vm : azure.virtualMachines().listByResourceGroup(resourceGroupName)) {
      LOG.info("VM: {}, provisioning state: {}, power state: {}", vm.id(), vm.provisioningState(),
          vm.powerState());
      if (vm.provisioningState().toLowerCase().contains("fail")) {
        resourceIds.add(vm.id());
      }
      Map<String, String> tags = vm.tags();
      if ((tags != null) && (tagName != null) && (tagValue != null)) {
        String vmTagValue = tags.get(tagName);
        if ((vmTagValue != null) && (vmTagValue.equals(tagValue))) {
          vmIds.add(vm.id());
        }
      }
    }
    if (!resourceIds.isEmpty()) {
      LOG.info("Delete failed VMs: {}", resourceIds);
      allocator.asyncDeleteByIdHelper(azure.virtualMachines(), "Virtual Machines",
          resourceIds, deleteFailedBuilder);
      resourceIds.clear();
    }
    if (!vmIds.isEmpty()) {
      LOG.info("Delete VMs with tag key/value pair {}/{} : {}", tagName, tagValue, vmIds);
      allocator.asyncDeleteByIdHelper(azure.virtualMachines(), "Virtual Machines",
          vmIds, deleteFailedBuilder);
    }

    // Find all orphaned Managed Disks and delete them
    for (Disk d : azure.disks().listByResourceGroup(resourceGroupName)) {
      if (!d.isAttachedToVirtualMachine()) {
        LOG.info("Found orphaned Managed Disk: {}", d.id());
        resourceIds.add(d.id());
      }
    }
    if (!resourceIds.isEmpty()) {
      LOG.info("Delete orphaned Disks");
      allocator.asyncDeleteByIdHelper(azure.disks(), "Managed Disks", resourceIds,
          deleteFailedBuilder);
      resourceIds.clear();
    }

    // Find all orphaned NICs and delete them
    for (NetworkInterface nic : azure.networkInterfaces().listByResourceGroup(resourceGroupName)) {
      if (nic.virtualMachineId() == null) {
        LOG.info("Found orphaned NIC: {}", nic.id());
        resourceIds.add(nic.id());
      }
    }
    if (!resourceIds.isEmpty()) {
      LOG.info("Delete orphaned NICs");
      allocator.asyncDeleteByIdHelper(azure.networkInterfaces(), "Network Interfaces",
          resourceIds, deleteFailedBuilder);
      resourceIds.clear();
    }

    // Find all orphaned PublicIPs and delete them
    for (PublicIPAddress p : azure.publicIPAddresses().listByResourceGroup(resourceGroupName)) {
      if (!p.hasAssignedNetworkInterface()) {
        LOG.info("Found orphaned PublicIP: {}", p.id());
        resourceIds.add(p.id());
      }
    }
    if (!resourceIds.isEmpty()) {
      LOG.info("Delete orphaned PublicIPs");
      allocator.asyncDeleteByIdHelper(azure.publicIPAddresses(), "Public IPs",
          resourceIds, deleteFailedBuilder);
      resourceIds.clear();
    }
  }

  private static void cleanUpVirtualMachineScaleSets(
      Azure azure,
      AzureCredentials credentials,
      String resourceGroupName,
      String tagName,
      String tagValue) {

    VirtualMachineScaleSetAllocator allocator = new VirtualMachineScaleSetAllocator(
        azure, credentials.getMsiManager(), (p, c) -> "");

    AzureComputeInstanceTemplate template = mock(AzureComputeInstanceTemplate.class);
    when(template.getConfigurationValue(eq(COMPUTE_RESOURCE_GROUP), any(LocalizationContext.class)))
        .thenReturn(resourceGroupName);

    azure.virtualMachineScaleSets().listByResourceGroup(resourceGroupName)
        .stream()
        .filter(vmss -> vmss.tags() != null && tagName != null && Objects.equals(vmss.tags().get(tagName), tagValue))
        .forEach(vmss -> {
          LOG.info("Deleting virtual machine scale set {} with tag {} -> {}", vmss.name(), tagName, tagValue);
          when(template.getGroupId()).thenReturn(vmss.name());

          try {
            allocator.delete(
                new DefaultLocalizationContext(Locale.getDefault(), ""),
                template,
                Collections.emptyList());
            LOG.info("Virtual machine scale set {} has been deleted", vmss.name());
          } catch (InterruptedException e) {
            LOG.warn("Virtual machine scale set {} might not have been deleted", vmss.name(), e);
          }
        });
  }
}
