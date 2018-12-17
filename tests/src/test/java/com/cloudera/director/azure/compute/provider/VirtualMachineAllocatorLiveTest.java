/*
 *
 * Copyright (c) 2017 Cloudera, Inc.
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

import static com.cloudera.director.azure.AzureExceptions.AZURE_ERROR_CODE;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getDnsName;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getVmName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.AzureExceptions;
import com.cloudera.director.azure.CustomVmImageTestHelper;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.Disk;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.DiskSkuTypes;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.msi.implementation.MSIManager;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.Network;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.PublicIPAddress;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.fluentcore.collection.SupportsDeletingById;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.StorageAccountSkuType;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.StorageAccount;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v2.provider.CloudProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.function.BiFunction;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AzureComputeProvider fault injection tests.
 *
 * These tests are live tests with some sections mocked to inject faults that happen in actual use.
 */
public class VirtualMachineAllocatorLiveTest extends AzureComputeProviderLiveTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(VirtualMachineAllocatorLiveTest.class);

  // Fields used by checks
  private static final String TEMPLATE_NAME = "LiveTestInstanceTemplate";
  private static final Map<String, String> TAGS = TestHelper.buildTagMap();
  private static final DefaultLocalizationContext DEFAULT_LOCALIZATION_CONTEXT =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Test
  public void allocateMoreThanRequiredLessThanRequestedExpectSuccess() throws Exception {
    LOG.info("allocateMoreThanRequiredLessThanRequestedExpectSuccess");

    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    // spy on AzureComputeProvider
    SimpleConfiguration config = TestHelper.buildValidDirectorLiveTestConfig();
    AzureComputeProvider provider = spy((AzureComputeProvider)
        cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(), config));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        config, TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    VirtualMachineAllocator allocator = spy(new VirtualMachineAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue));

    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(MSIManager.class), any(BiFunction.class));

    // the three VMs to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // cause one create to fail
    Mockito.doThrow(new RuntimeException("Force failure."))
        .doCallRealMethod() // real method called for the rest
        .when(allocator)
        .buildVirtualMachineCreatable(
            any(Azure.class),
            any(LocalizationContext.class),
            any(AzureComputeInstanceTemplate.class),
            ArgumentMatchers.anyString(),
            any(AvailabilitySet.class),
            any(Network.class),
            any(NetworkSecurityGroup.class));


    // 1. allocate
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size() - 2);

    // 2. verify two instances were created
    LOG.info("2. verify successful instance count");
    Assert.assertTrue(provider.find(template, instanceIds).size() == instanceIds.size() - 1);

    // 3. delete
    LOG.info("3. delete");
    provider.delete(template, instanceIds);
  }

  @Test
  public void allocateLessThanRequiredExpectNoResourceLeaked() throws Exception {
    LOG.info("allocateLessThanRequiredExpectNoResourceLeaked");

    // 0. set up
    LOG.info("0. set up");
    // so we default to azure-plugin.conf
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider)
        cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
            TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the single VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));


    // 1. allocate
    LOG.info("1. allocate");
    boolean caughtException = false;
    try {
      provider.allocate(template, instanceIds, instanceIds.size() + 1);
    } catch (UnrecoverableProviderException e) {
      LOG.info("Caught expected exception: ", e);
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // 2. verify no instances were created
    LOG.info("2. verify successful instance count");
    Assert.assertTrue(provider.find(template, instanceIds).size() == 0);
  }

  @Test
  public void allocateWithDirector25NamingSchemeExpectNoResourcesLeaked() throws Exception {
    LOG.info("allocateWithDirector25NamingSchemeExpectNoResourcesLeaked");

    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "No");
    SimpleConfiguration config = new SimpleConfiguration(map);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        config, Locale.getDefault());
    AzureComputeProvider provider = spy((AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig()));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        config, TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    VirtualMachineAllocator allocator = spy(new VirtualMachineAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue));
    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(MSIManager.class), any(BiFunction.class));

    // the single VM to use for this test
    String instanceId = UUID.randomUUID().toString();
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(instanceId);
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    LOG.info("1. allocate");
    // custom Storage Account
    String uniqueSaName = getFirstGroupOfUuid(UUID.randomUUID().toString());
    StorageAccount.DefinitionStages.WithCreate storageAccountCreatable = azure
        .storageAccounts()
        .define(uniqueSaName)
        .withRegion(TestHelper.TEST_REGION)
        .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
        .withSku(StorageAccountSkuType.PREMIUM_LRS);
    doReturn(storageAccountCreatable)
        .when(allocator)
        .buildStorageAccountCreatable(
            any(Azure.class),
            any(LocalizationContext.class),
            any(AzureComputeInstanceTemplate.class),
            ArgumentMatchers.anyString());

    // custom Public IP
    String uniquePipUuid = UUID.randomUUID().toString();
    String uniquePipName = getFirstGroupOfUuid(uniquePipUuid);
    PublicIPAddress.DefinitionStages.WithCreate publicIpCreatable = azure
        .publicIPAddresses()
        .define(uniquePipName)
        .withRegion(TestHelper.TEST_REGION)
        .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
        .withLeafDomainLabel(getDnsName(uniquePipUuid, template.getInstanceNamePrefix()));
    doReturn(publicIpCreatable)
        .when(allocator)
        .buildPublicIpCreatable(
            any(Azure.class),
            any(LocalizationContext.class),
            any(AzureComputeInstanceTemplate.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString());

    // custom Network Interface
    String uniqueNicName = getFirstGroupOfUuid(UUID.randomUUID().toString());
    Network vnet = azure.networks().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, TestHelper.TEST_VIRTUAL_NETWORK);
    NetworkSecurityGroup nsg = azure.networkSecurityGroups().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, TestHelper.TEST_NETWORK_SECURITY_GROUP);
    NetworkInterface.DefinitionStages.WithCreate nicCreatable = azure
        .networkInterfaces()
        .define(uniqueNicName)
        .withRegion(TestHelper.TEST_REGION)
        .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
        .withExistingPrimaryNetwork(vnet)
        .withSubnet("default")
        // AZURE_SDK there is no way to set the IP allocation method to static
        .withPrimaryPrivateIPAddressDynamic()
        .withExistingNetworkSecurityGroup(nsg);
    if (map.get(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey()).equals("Yes")) {
        nicCreatable = nicCreatable.withNewPrimaryPublicIPAddress(publicIpCreatable);
    }
    doReturn(nicCreatable)
        .when(allocator)
        .buildNicCreatable(
            any(Azure.class),
            any(LocalizationContext.class),
            any(AzureComputeInstanceTemplate.class),
            ArgumentMatchers.anyString(),
            any(Network.class),
            any(NetworkSecurityGroup.class),
            ArgumentMatchers.anyString());

    Collection<? extends AzureComputeInstance<?>> allocatedInstances =
        provider.allocate(template, instanceIds, instanceIds.size());
    // verify that allocate returns a correctly sized list
    Assert.assertEquals(instanceIds.size(), allocatedInstances.size());

    LOG.info("2. verify that the custom resources with unique names exist");
    StorageAccount sa = azure.storageAccounts().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniqueSaName);
    Assert.assertNotNull(sa);
    NetworkInterface ni = azure.networkInterfaces().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniqueNicName);
    Assert.assertNotNull(ni);
    PublicIPAddress pip = azure.publicIPAddresses().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniquePipName);
    Assert.assertNotNull(pip);


    LOG.info("3. delete");
    provider.delete(template, instanceIds);

    LOG.info("4. verify that the custom resources with unique names have been deleted");
    sa = azure.storageAccounts().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniqueSaName);
    Assert.assertNull(sa);
    ni = azure.networkInterfaces().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniqueNicName);
    Assert.assertNull(ni);
    pip = azure.publicIPAddresses().getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, uniquePipName);
    Assert.assertNull(pip);
  }

  @Test
  public void allocateLessThanRequiredWithVmCreatableExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithVmCreatableExceptionExpectNoResourcesLeaked");

    clientSideCreatableFaultInjectionHelper(true, false, false, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithManagedDiskCreatableExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithManagedDiskCreatableExceptionExpectNoResourcesLeaked");

    clientSideCreatableFaultInjectionHelper(false, true, false, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithStorageAccountCreatableExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithStorageAccountCreatableExceptionExpectNoResourcesLeaked");

    clientSideCreatableFaultInjectionHelper(false, false, true, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithNicCreatableExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithNicCreatableExceptionExpectNoResourcesLeaked");

    clientSideCreatableFaultInjectionHelper(false, false, false, true, false);
  }

  @Test
  public void allocateLessThanRequiredWithPublicIpCreatableExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithPublicIpCreatableExceptionExpectNoResourcesLeaked");

    clientSideCreatableFaultInjectionHelper(false, false, false, false, true);
  }

  /**
   * This method stresses the allocate cleanup logic by forcing client side creatables to throw exceptions when
   * they're being built, which in turn causes allocate to throw a UnrecoverableProviderException
   * because not enough VMs were created. Then the VM cleanup logic is tested to verify that all VMs
   * and their resources are cleaned up.
   *
   * This is a client side fault injection because the creatables throw an exception before they
   * hit Azure.
   *
   * Note: while the test takes any combination of parameters it has only been tested with one at a
   * time.
   *
   * @param failVm whether to fail the VM creatable
   * @param failManagedDisks whether to fail the Managed Disks creatable
   * @param failStorageAccount  whether to fail the Storage Account creatable
   * @param failNic whether to fail the Nic creatable
   * @param failPublicIp whether to fail the Public IP creatable
   * @throws Exception if test fails unexpectedly
   */
  private void clientSideCreatableFaultInjectionHelper(boolean failVm, boolean failManagedDisks,
      boolean failStorageAccount, boolean failNic, boolean failPublicIp)
      throws Exception {
    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    if (failStorageAccount) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
          "No");
    }
    SimpleConfiguration config = new SimpleConfiguration(map);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());

    // spy on AzureComputeProvider
    AzureComputeProvider provider = spy((AzureComputeProvider)
        cloudProvider.createResourceProvider(
            AzureComputeProvider.METADATA.getId(), config));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        config, TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    VirtualMachineAllocator allocator = spy(new VirtualMachineAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue));

    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(MSIManager.class), any(BiFunction.class));

    // the three VMs to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // cause one create to fail
    if (failVm) {
      // throw on the first call then default to calling the actual method for the rest
      Mockito.doThrow(new RuntimeException("Force failure."))
          .doCallRealMethod()
          .when(allocator)
          .buildVirtualMachineCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString(),
              any(AvailabilitySet.class),
              any(Network.class),
              any(NetworkSecurityGroup.class));
    }

    if (failNic) {
      // throw on the first call then default to calling the actual method for the rest
      Mockito.doThrow(new RuntimeException("Force failure."))
          .doCallRealMethod()
          .when(allocator)
          .buildNicCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString(),
              any(Network.class),
              any(NetworkSecurityGroup.class),
              ArgumentMatchers.anyString());
    }

    if (failPublicIp) {
      // throw on the first call then default to calling the actual method for the rest
      Mockito.doThrow(new RuntimeException("Force failure."))
          .doCallRealMethod()
          .when(allocator)
          .buildPublicIpCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString(),
              ArgumentMatchers.anyString());
    }

    if (failManagedDisks) {
      // throw on the first call then default to calling the actual method for the rest
      Mockito.doThrow(new RuntimeException("Force failure."))
          .doCallRealMethod()
          .when(allocator)
          .buildManagedDiskCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString());
    }

    if (failStorageAccount) {
      // throw on the first call then default to calling the actual method for the rest
      Mockito.doThrow(new RuntimeException("Force failure."))
          .doCallRealMethod()
          .when(allocator)
          .buildStorageAccountCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString());
    }

    // 1. allocate - everything should clean up automatically
    LOG.info("1. allocate");
    try {
      provider.allocate(template, instanceIds, instanceIds.size());
    } catch (UnrecoverableProviderException e) {
      LOG.info("1.b. allocate failed on purpose");
    }

    // 2. verify cleanup - no resources should be orphaned
    LOG.info("2. verify cleanup");
    boolean cleanedUp = true; // used to check if anything failed to clean up

    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(provider.getLocalizationContext());
    String rgName = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        templateLocalizationContext);

    for (String instanceId : instanceIds) {
      String commonResourceNamePrefix = getFirstGroupOfUuid(instanceId);
      // vm
      String vmName = getVmName(instanceId, template.getInstanceNamePrefix());
      if (azure.virtualMachines().getByResourceGroup(rgName, instanceId) != null) {
        LOG.error("Failed to clean up VM {}", vmName);
        cleanedUp = false;
      }

      // storage
      if (failManagedDisks) {
        String osDisk = commonResourceNamePrefix + "-OS";
        if (azure.disks().getByResourceGroup(rgName, osDisk) != null) {
          LOG.error("Failed to clean up MD {}", osDisk);
        }
        int dataDiskCount = Integer.parseInt(template.getConfigurationValue(
            AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT,
            templateLocalizationContext));
        for (int n = 0; n < dataDiskCount; n += 1) {
          String dataDisk = commonResourceNamePrefix + "-" + n;
          if (azure.disks().getByResourceGroup(rgName, dataDisk) != null) {
            LOG.error("Failed to clean up MD {}", dataDisk);
            cleanedUp = false;
          }
        }
      } else {
        if (azure.storageAccounts().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
          LOG.error("Failed to clean up SA {}", commonResourceNamePrefix);
          cleanedUp = false;
        }
      }

      // nic
      if (azure.networkInterfaces().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
        LOG.error("Failed to clean up Nic {}", commonResourceNamePrefix);
      }

      // public IP
      if (azure.publicIPAddresses().getByResourceGroup(rgName, commonResourceNamePrefix) != null) {
        LOG.error("Failed to clean up Public IP {}", commonResourceNamePrefix);
      }
    }
    LOG.info("Resources cleaned up? {}", cleanedUp);

    // 3. delete (just-in-case cleanup)
    LOG.info("3. delete");
    provider.delete(template, instanceIds);

    // 4. Fail test if cleanup didn't work
    Assert.assertTrue("Test has failed to clean up all resources.", cleanedUp);
  }

  @Test
  public void allocateLessThanRequiredWithVmBackendExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithVmBackendExceptionExpectNoResourcesLeaked");

    backendFaultInjectionHelper(true, false, false, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithManagedDiskBackendExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithManagedDiskBackendExceptionExpectNoResourcesLeaked");

    backendFaultInjectionHelper(false, true, false, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithStorageAccountBackendExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithStorageAccountBackendExceptionExpectNoResourcesLeaked");

    backendFaultInjectionHelper(false, false, true, false, false);
  }

  @Test
  public void allocateLessThanRequiredWithNicBackendExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithNicBackendExceptionExpectNoResourcesLeaked");

    backendFaultInjectionHelper(false, false, false, true, false);
  }

  @Test
  public void allocateLessThanRequiredWithPublicIpBackendExceptionExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("allocateLessThanRequiredWithPublicIpBackendExceptionExpectNoResourcesLeaked");

    backendFaultInjectionHelper(false, false, false, false, true);
  }

  /**
   * This method stresses the allocate cleanup logic by forcing backend failures at different points
   * along the VM + asset creation pipeline. This means that some of the VM's assets will have been
   * created before a failure is forced, stressing the cleanup logic for partially created VMs.
   *
   * Failures are forced through different techniques depending on the resource:
   * - VM: have the VM use an incompatible Availability Set
   * - Managed Disk: pre-create a MD, then have MD in question use the same name, forcing a name
   *   collision
   * - Storage Account: pre-create a SA, then have the SA in question use the same name, forcing a
   *   name collision
   * - Nic: pre-create a nic, then have the Nic in question use the same name, forcing a name
   *   collision
   * - Public IP: pre-create a Public IP, then have the Public IP in question use the same name,
   *   forcing a name collision
   *
   * Note: while the test takes any combination of parameters it has only been tested with one at a
   * time.
   *
   * @param failVm whether to fail the VM creation on the backend
   * @param failManagedDisks whether to fail the Managed Disks creation on the backend
   * @param failStorageAccount whether to fail the Storage Account creation on the backend
   * @param failNic whether to fail the Nic creation on the backend
   * @param failPublicIp whether to fail the Public IP creation on the backend
   * @throws Exception if test fails unexpectedly
   */
  private void backendFaultInjectionHelper(boolean failVm, boolean failManagedDisks,
      boolean failStorageAccount, boolean failNic, boolean failPublicIp) throws Exception {
    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    if (failManagedDisks) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap()
          .getConfigKey(), "1");
    }
    if (failStorageAccount) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
          "No");
    }
    SimpleConfiguration config = new SimpleConfiguration(map);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        config, Locale.getDefault());

    // spy on AzureComputeProvider
    AzureComputeProvider provider = spy((AzureComputeProvider)
        cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
            config));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        config, TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    VirtualMachineAllocator allocator = spy(new VirtualMachineAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue));

    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(MSIManager.class), any(BiFunction.class));

    // 1. allocate one instance
    LOG.info("1. allocate");

    Collection<String> firstPassInstanceIds = new ArrayList<>();
    String firstPassId = UUID.randomUUID().toString();
    firstPassInstanceIds.add(firstPassId);
    LOG.info("Using these UUIDs: {}", Arrays.toString(firstPassInstanceIds.toArray()));

    provider.allocate(template, firstPassInstanceIds, firstPassInstanceIds.size());

    // 2. allocate another instance but force a backend failure
    LOG.info("2. allocate");

    Collection<String> secondPassInstanceIds = new ArrayList<>();
    String secondPassId = UUID.randomUUID().toString();
    secondPassInstanceIds.add(secondPassId);

    // fail VM create by having it use an incompatible AS
    if (failVm) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
          "Yes");

      template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
          new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    }

    // fail managed disks create by having it use the name as the existing ones
    if (failManagedDisks) {
      Disk.DefinitionStages.WithCreate mdCreatable = azure
          .disks()
          .define(getFirstGroupOfUuid(firstPassId) + "-0")
          .withRegion(TestHelper.TEST_REGION)
          .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
          .withData()
          .withSizeInGB(512)
          .withSku(DiskSkuTypes.PREMIUM_LRS);

      doReturn(mdCreatable)
          .when(allocator)
          .buildManagedDiskCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString());
    }

    // fail storage account create by having it use an invalid name (Storage account name must be
    // between 3 and 24 characters in length and use numbers and lower-case letters only).
    if (failStorageAccount) {
      StorageAccount.DefinitionStages.WithCreate storageAccountCreatable = azure
          .storageAccounts()
          .define("-INVALID-INVALID-INVALID-")
          .withRegion(TestHelper.TEST_REGION)
          .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
          .withSku(SkuName.PREMIUM_LRS);

      doReturn(storageAccountCreatable)
          .when(allocator)
          .buildStorageAccountCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString());
    }

    // fail nic create by having it use the same name as the existing nic
    if (failNic) {
      Network vnet = azure.networks()
          .getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, TestHelper.TEST_VIRTUAL_NETWORK);
      NetworkSecurityGroup nsg = azure.networkSecurityGroups().getByResourceGroup(
          TestHelper.TEST_RESOURCE_GROUP, TestHelper.TEST_NETWORK_SECURITY_GROUP);
      NetworkInterface.DefinitionStages.WithCreate nicCreatable = azure
          .networkInterfaces()
          .define(getFirstGroupOfUuid(firstPassId))
          .withRegion(TestHelper.TEST_REGION)
          .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
          .withExistingPrimaryNetwork(vnet)
          .withSubnet(TestHelper.TEST_SUBNET)
          // AZURE_SDK there is no way to set the IP allocation method to static
          .withPrimaryPrivateIPAddressDynamic()
          .withExistingNetworkSecurityGroup(nsg);

      // don't create a public IP collision
      boolean createPublicIp = map.get(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP
          .unwrap().getConfigKey()).equalsIgnoreCase("yes");
      if (createPublicIp) {
        nicCreatable = nicCreatable.withNewPrimaryPublicIPAddress(allocator.buildPublicIpCreatable(
            azure, provider.getLocalizationContext(), template, secondPassId, getFirstGroupOfUuid(firstPassId)));
      }

      doReturn(nicCreatable)
          .when(allocator)
          .buildNicCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString(),
              any(Network.class),
              any(NetworkSecurityGroup.class),
              ArgumentMatchers.anyString());
    }

    // fail public IP create by having it use the same name as the existing public IP
    if (failPublicIp) {
      PublicIPAddress.DefinitionStages.WithCreate publicIpCreatable = azure
          .publicIPAddresses()
          .define(getFirstGroupOfUuid(firstPassId))
          .withRegion(TestHelper.TEST_REGION)
          .withExistingResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
          .withLeafDomainLabel(getDnsName(firstPassId, template.getInstanceNamePrefix()));

      doReturn(publicIpCreatable)
          .when(allocator)
          .buildPublicIpCreatable(
              any(Azure.class),
              any(LocalizationContext.class),
              any(AzureComputeInstanceTemplate.class),
              ArgumentMatchers.anyString(),
              ArgumentMatchers.anyString());
    }

    // call allocate for the second time, this time it will fail
    boolean allocateFailedOnPurpose = false;
    try {
      provider.allocate(template, secondPassInstanceIds, secondPassInstanceIds.size());
      LOG.error("Allocate did not fail (it should have)");
    } catch (UnrecoverableProviderException e) {
      LOG.info("2.b. allocate failed on purpose");
      allocateFailedOnPurpose = true;
    }

    // 3. verify cleanup - no resources should be orphaned
    LOG.info("3. verify cleanup");
    boolean cleanedUp = AzureComputeProviderLiveTestHelper
        .resourcesDeleted(azure, provider, template, secondPassInstanceIds);
    if (!cleanedUp) {
      LOG.error("Test did not clean up all resources.");
    }

    // 4. delete the first group and just-in-case cleanup the second group
    LOG.info("4. delete");
    provider.delete(template, firstPassInstanceIds);
    provider.delete(template, secondPassInstanceIds);

    // 5. fail the test if the second allocate worked and if the test cleanup didn't work
    if (!allocateFailedOnPurpose && !cleanedUp) {
      Assert.fail("The second allocate did not fail (it should have); and the test has failed to " +
          "clean up all resources.");
    }

    // 5.a. fail the test if the second allocate worked
    Assert.assertTrue("The second allocate did not fail (it should have).",
        allocateFailedOnPurpose);

    // 5.b. fail the test if cleanup didn't work
    Assert.assertTrue("Test has failed to clean up all resources.", cleanedUp);
  }

  @Test
  public void deleteNotAllVirtualMachinesExpectExceptionThrown() throws Exception {
    LOG.info("deleteNotAllVirtualMachinesExpectExceptionThrown");
    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap().getConfigKey(), "1");
    SimpleConfiguration config = new SimpleConfiguration(map);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        config, Locale.getDefault());

    // spy on AzureComputeProvider
    AzureComputeProvider provider = spy((AzureComputeProvider)
        cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(), config));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        config, TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    VirtualMachineAllocator allocator = spy(new VirtualMachineAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue));

    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(MSIManager.class), any(BiFunction.class));

    // 1. allocate one instance
    LOG.info("1. allocate");
    Collection<String> instanceIds = new ArrayList<>();
    String instanceId = UUID.randomUUID().toString();
    instanceIds.add(instanceId);
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. On purpose fail to delete just one of the VM's Managed Disks to verify that errors have been accumulated
    StringBuilder deleteFailedBuilder = new StringBuilder();
    List<String> resourceIds = new ArrayList<>();
    resourceIds.add("/subscriptions/" + TestHelper.LIVE_TEST_SUBSCRIPTION_ID + "/resourceGroups/" +
        TestHelper.TEST_RESOURCE_GROUP + "/providers/Microsoft.Compute/disks/" +
        getFirstGroupOfUuid(instanceId) + "-0");

    boolean successful = allocator.asyncDeleteByIdHelper(azure.disks(), "Managed Disks", resourceIds,
        deleteFailedBuilder);
    LOG.info(deleteFailedBuilder.toString());

    // 3. fake delete
    boolean deleteFailedOnPurpose = false;
    doReturn(false)
        .when(allocator)
        .asyncDeleteByIdHelper(any(SupportsDeletingById.class), ArgumentMatchers.anyString(),
            any(List.class), any(StringBuilder.class));

    try {
      LOG.info("Fake delete.");
      provider.delete(template, instanceIds);
    } catch (UnrecoverableProviderException e) {
      Assert.assertEquals("Failed to delete cluster resources due to an Azure provider error. " +
          "These resources may still be running. Try deleting the cluster again, or contact support if this error persists.",
          e.getMessage());
      deleteFailedOnPurpose = true;
      LOG.info("Failed delete correctly threw an UnrecoverableProviderException.");
    } catch (Exception e) {
      LOG.error("Failed delete did not throw an UnrecoverableProviderException. It threw this instead: ", e);
    }

    // 4. actual delete
    LOG.info("Real delete.");
    Mockito.doCallRealMethod()
        .when(allocator)
        .asyncDeleteByIdHelper(any(SupportsDeletingById.class), ArgumentMatchers.anyString(),
            any(List.class), any(StringBuilder.class));

    provider.delete(template, instanceIds);

    // 5. verify cleanup - no resources should be orphaned
    LOG.info("Verify cleanup");
    boolean cleanedUp = AzureComputeProviderLiveTestHelper.resourcesDeleted(azure, provider, template, instanceIds);
    if (!cleanedUp) {
      LOG.error("Test did not clean up all resources.");
    }

    // 5.a. fail the test if the string builder passed to asyncDeleteByIdHelper is empty
    if (deleteFailedBuilder.toString().trim().isEmpty()) {
      Assert.fail("The error accumulator passed to asyncDeleteByIdHelper is empty (it should contain error contents");
    }

    // 5.b. fail the test if the delete didn't fail on purpose and if the test cleanup didn't work
    if (!deleteFailedOnPurpose && !cleanedUp) {
      Assert.fail("The failed delete did not throw an UnrecoverableProviderException (it should have); and the test " +
          "has failed to clean up all resources.");
    }

    // 5.c. fail the test if the delete didn't throw the correct exception
    Assert.assertTrue("The failed delete did not throw an UnrecoverableProviderException (it should have).",
        deleteFailedOnPurpose);

    // 5.d. verify asyncDeleteByIdHelper
    Assert.assertFalse("The direct call to asyncDeleteByIdHelper was successful (it should not have been).",
        successful);

    // 5.e. fail the test if cleanup didn't work
    Assert.assertTrue("Test has failed to clean up all resources.", cleanedUp);
  }

  @Test
  public void createVmWithCustomImageMissingOrWrongPlanErrorNoResourcesLeaked() throws Exception {
    LOG.info("createVmWithCustomImageMissingPlanErrorNoResourcesLeaked");

    // Create a valid custom image for testing
    LOG.info("0. create custom image for test");
    String customImageName = "test-custom-image-" + UUID.randomUUID();
    String customImageUri = CustomVmImageTestHelper.buildCustomManagedVmImage(
        customImageName, true);

    // create config with no custom image purchase plan
    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        customImageUri);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate templateWithoutPlan = new AzureComputeInstanceTemplate(
        TEMPLATE_NAME, new SimpleConfiguration(cfgMap), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // create another config with the wrong plan (test custom image uses CentOS 6_7)
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
        .getConfigKey(), "/publisher/cloudera/product/cloudera-centos-os/name/7_2");
    AzureComputeInstanceTemplate templateWithWrongPlan = new AzureComputeInstanceTemplate(
        TEMPLATE_NAME, new SimpleConfiguration(cfgMap), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate the VM the first time
    LOG.info("1. allocate vm with custom image but missing purchase plan");
    try {
      provider.allocate(templateWithoutPlan, instanceIds, instanceIds.size());
    } catch (UnrecoverableProviderException e) {
      LOG.info("Caught expected exception: ", e);
    }

    // 2. verify that no instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances1 = provider.find(templateWithoutPlan,
        instanceIds);
    Assert.assertEquals(0, foundInstances1.size());

    // 3. verify there is no resource leak
    LOG.info("3. verify cleanup");
    if (!AzureComputeProviderLiveTestHelper.resourcesDeleted(azure, provider, templateWithoutPlan, instanceIds)) {
      // delete VM image used for testing before failing the test
      CustomVmImageTestHelper.deleteCustomManagedVmImage(customImageUri);
      Assert.fail("VM resource verification failed.");
    }

    // 4. allocate the VM the first time
    LOG.info("4. allocate vm with custom image but with wrong purchase plan");
    try {
      provider.allocate(templateWithWrongPlan, instanceIds, instanceIds.size());
    } catch (UnrecoverableProviderException e) {
      LOG.info("Caught expected exception: ", e);
    }

    // 5. verify that no instances can be found
    LOG.info("5. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances2 = provider.find(templateWithWrongPlan,
        instanceIds);
    Assert.assertEquals(0, foundInstances2.size());

    // 6. verify there is no resource leak
    LOG.info("6. verify cleanup");
    if (!AzureComputeProviderLiveTestHelper.resourcesDeleted(azure, provider, templateWithoutPlan, instanceIds)) {
      // delete VM image used for testing before failing the test
      CustomVmImageTestHelper.deleteCustomManagedVmImage(customImageUri);
      Assert.fail("VM resource verification failed.");
    }

    // delete the custom image created for test
    CustomVmImageTestHelper.deleteCustomManagedVmImage(customImageUri);
  }

  @Test
  public void createVmWithEULANotAcceptedImageExpectNoResourcesLeaked()
      throws Exception {
    LOG.info("createVmWithEULANotAcceptedImageExpectNoResourcesLeaked");

    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    // EULA for this image has not been accepted.
    // This test will fail if it is accepted in the future.
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-altus-centos-os-preview/sku/cmcdh_5-15/version/latest");
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the repeated instanceId to use for this test
    String instanceId = UUID.randomUUID().toString();
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(instanceId);

    // 1. allocate the VM which will fail to come up
    LOG.info("1. allocate");
    try {
      provider.allocate(template, instanceIds, 1);
    } catch (UnrecoverableProviderException e) {
      LOG.info("Caught expected UnrecoverableProviderException: {}",
          e.getDetails().getConditionsByKey().get(null));
      verifySingleErrorCode(e, AzureExceptions.IMAGE_EULA_NOT_ACCEPTED);
    }

    // 2. verify that no instance is created
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(foundInstances.size(), 0);

    // 3. verify cleanup - no resources should be orphaned
    LOG.info("3. verify cleanup");
    Assert.assertTrue(AzureComputeProviderLiveTestHelper.resourcesDeleted(azure, provider, template, instanceIds));
  }

  private void verifySingleErrorCode(AbstractPluginException ex, String expectedErrorCode) {
    Map<String, SortedSet<PluginExceptionCondition>> conditionsByKey =
        ex.getDetails().getConditionsByKey();
    SortedSet<PluginExceptionCondition> conditions = conditionsByKey.get(null);
    PluginExceptionCondition condition = Iterables.getOnlyElement(conditions);
    Map<String, String> exceptionInfo = condition.getExceptionInfo();
    Assert.assertEquals(exceptionInfo.get(AZURE_ERROR_CODE), expectedErrorCode);
  }
}
