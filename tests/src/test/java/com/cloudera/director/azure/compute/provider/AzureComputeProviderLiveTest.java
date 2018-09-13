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

import static com.cloudera.director.azure.compute.provider.AzureComputeProviderLiveTestHelper.getStorageAccountNameFromStorageProfile;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.MANAGED_OS_DISK_SUFFIX;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getVmName;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.AUTOMATIC;
import static com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.GROUP_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.AzureCreator;
import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.CustomVmImageTestHelper;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.instance.AzureInstance;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ResourceIdentityType;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineDataDisk;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSetVM;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.ActiveDirectoryObject;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.ServicePrincipal;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.IPAllocationMethod;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkInterfaceBase;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.azure.shaded.org.apache.commons.lang3.RandomStringUtils;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.InstanceStatus;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.CloudProvider;
import com.cloudera.director.spi.v2.provider.Launcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AzureComputeProvider live tests.
 *
 * These tests exercise the Azure Plugin through the Director SPI interface in
 * AzureComputeProvider.class / AbstractComputeProvider.class with the methods:
 * - allocate()
 * - find()
 * - getInstanceState()
 * - delete()
 */
public class AzureComputeProviderLiveTest extends AzureComputeProviderLiveTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProviderLiveTest.class);

  private static final String TEMPLATE_NAME = "LiveTestInstanceTemplate";
  private static final Map<String, String> TAGS = TestHelper.buildTagMap();

  private static final DefaultLocalizationContext DEFAULT_LOCALIZATION_CONTEXT =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      // Print out the method name before each test
      LOG.info("Running test: '{}'.", description.getMethodName());
    }
  };

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void reset() throws Exception {
    LOG.info("reset");

    LAUNCHER.initialize(new File("non_existent_file"), null);
  }

  /**
   * Basic pre-commit full cycle sanity live test that runs both VirtualMachine and
   * VirtualMachineScaleSet tests. It's similar to the other fullCycle tests but left standalone
   * for ease of remembering which test to run before committing.
   *
   * @throws Exception
   */
  @Test
  public void fullCycle() throws Exception {
    LOG.info("Full Cycle VM.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setPublicIP(false)
        .build());

    LOG.info("Full Cycle VMSS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setNumberOfVMs(1)
        .build());
  }

  @Test
  public void fullCycleVM() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(3)
        .build());
  }

  @Test
  public void fullCycleVMWithUserAssignedMsiExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUserAssignedMsiName(TestHelper.TEST_USER_ASSIGNED_MSI_NAME)
        .setUserAssignedMsiResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
        .build());
  }

  @Test
  public void fullCycleVMWithDynamicPrivateIpAddressAndAcceleratedNetworkingExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setWithStaticPrivateIpAddress(false)
        .setWithAcceleratedNetworking(true)
        .build());
  }

  @Test
  public void fullCycleVMWithNoHostFqdnSuffixExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setHostFqdnSuffix("")
        .build());
  }

  @Test
  public void fullCycleVMWithVariousValidStorageAndAvailabilitySetCombinationsExpectSuccess() throws Exception {
    // Combinations of Managed Disks and Storage Accounts, with both Premium and Standard storage,
    // with a random disk size between 1 and 4095, inclusive
    int randomSize = ThreadLocalRandom.current().nextInt(1, 4095 + 1);
    LOG.info("Using the randomly generated sized storage of {}.", randomSize);

    LOG.info("Running scenario: Managed Disks + Premium Storage + random size + \"\" (empty string) AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.PREMIUM_LRS.toString())
        .setManagedDisks(true)
        .setAvailabilitySet("")
        .build());

    LOG.info("Running scenario: Managed Disks + Standard Storage + random size + null AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.STANDARD_LRS.toString())
        .setManagedDisks(true)
        .setAvailabilitySet(null)
        .build());

    LOG.info("Running scenario: Managed Disks + Standard Storage + random size + managed AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.STANDARD_LRS.toString())
        .setManagedDisks(true)
        .setAvailabilitySet(TestHelper.TEST_AVAILABILITY_SET_MANAGED)
        .build());

    LOG.info("Running scenario: Storage Accounts + Premium Storage + random size + \"\" (empty string) AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.PREMIUM_LRS.toString())
        .setManagedDisks(false)
        .setAvailabilitySet("")
        .build());

    LOG.info("Running scenario: Storage Accounts + Standard Storage + random size + null AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.STANDARD_LRS.toString())
        .setManagedDisks(false)
        .setAvailabilitySet(null)
        .build());

    LOG.info("Running scenario: Storage Accounts + Standard Storage + random size + unmanaged AS.");
    fullCycleHelper(AzureCreator.newBuilder()
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.STANDARD_LRS.toString())
        .setManagedDisks(false)
        .setAvailabilitySet(TestHelper.TEST_AVAILABILITY_SET_UNMANAGED)
        .build());
  }

  @Test
  public void fullCycleVMWithDeprecatedStorageAccountTypeStringsExpectSuccess() throws Exception {
    // both Managed Disks and Storage Accounts need to be tested as Managed Disks will default to
    // Premium_LRS if the disk type is invalid, where Storage Accounts will throw an:
    //   java.lang.IllegalArgumentException: sku.name is required and cannot be null.

    // with Managed Disks
    for (String type : Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()) {
      fullCycleHelper(AzureCreator.newBuilder()
          .setManagedDisks(true)
          .setStorageAccountType(type)
          .build());
    }

    // with Storage Accounts
    for (String type : Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()) {
      fullCycleHelper(AzureCreator.newBuilder()
          .setManagedDisks(false)
          .setStorageAccountType(type)
          .build());
    }
  }

  @Test
  public void fullCycleVMWithVmImageOneLineExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setImage("/publisher/cloudera/offer/cloudera-centos-os/sku/7_2/version/latest")
        .build());

  }

  @Test
  public void fullCycleVMWithPreviewVmImageOneLineExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setImage("/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/latest")
        .build());
  }

  @Test
  public void fullCycleVMWithCustomDataEncoded() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setCustomDataEncoded(TestHelper.TEST_CUSTOM_DATA_ENCODED)
        .build());
  }

  @Test
  public void fullCycleVMWithCustomDataUnencoded() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setCustomDataUnecoded(TestHelper.TEST_CUSTOM_DATA_UNENCODED)
        .build());
  }

  /**
   * To set a custom user-agent via CLI set this flag:
   *   -DuserAgent=UUID
   */
  @Test
  public void fullCycleVMWithCustomUserAgent() throws Exception {
    // use the user-agent passed in via command line; no user-agent is used if the command line parameter is missing
    String userAgent = System.getProperty(AzureCredentialsConfiguration.USER_AGENT.unwrap().getConfigKey());

    fullCycleHelper(AzureCreator.newBuilder()
        .setUserAgent(userAgent)
        .setNumberOfVMs(1)
        .setPublicIP(false)
        .build());
  }

  @Test
  public void fullCycleVmss() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setNumberOfVMs(3)
        .build());
  }

  @Test
  public void fullCycleVmssWithUserAssignedMsiExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setUserAssignedMsiName(TestHelper.TEST_USER_ASSIGNED_MSI_NAME)
        .setUserAssignedMsiResourceGroup(TestHelper.TEST_RESOURCE_GROUP)
        .build());
  }

  @Test
  public void fullCycleVmssWithVariousValidStorageCombinationsExpectSuccess() throws Exception {
    // Managed Disks of random size between between 1 and 4095, inclusive
    String randomSize = Integer.toString(ThreadLocalRandom.current().nextInt(1, 4095 + 1));
    LOG.info("Using the randomly generated sized storage of {}.", randomSize);

    LOG.info("Managed Disks + Premium Storage + random size");
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.PREMIUM_LRS.toString())
        .setManagedDisks(true)
        .build());

    LOG.info("Managed Disks + Standard Storage + random size");
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setNumberOfVMs(1)
        .setDataDiskCount(1)
        .setDataDiskSize(randomSize)
        .setStorageAccountType(SkuName.STANDARD_LRS.toString())
        .setManagedDisks(true)
        .build());
  }

  @Test
  public void fullCycleVmssWithVMImageOneLineExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setImage("/publisher/cloudera/offer/cloudera-centos-os/sku/7_2/version/latest")
        .build());
  }

  @Test
  public void fullCycleVmssWithPreviewVMImageOneLineExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setImage("/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/latest")
        .build());
  }

  @Test
  public void fullCycleVmssWithCustomDataEncodedExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setCustomDataEncoded(TestHelper.TEST_CUSTOM_DATA_ENCODED)
        .build());
  }

  @Test
  public void fullCycleVmssWithCustomDataUnencodeExpectSuccess() throws Exception {
    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setCustomDataUnecoded(TestHelper.TEST_CUSTOM_DATA_UNENCODED)
        .build());
  }

  /**
   * To set a custom user-agent via CLI set this flag:
   *   -DuserAgent=UUID
   */
  @Test
  public void fullCycleVmssWithCustomUserAgent() throws Exception {
    // use the user-agent passed in via command line; no user-agent is used if the command line parameter is missing
    String userAgent = System.getProperty(AzureCredentialsConfiguration.USER_AGENT.unwrap().getConfigKey());

    fullCycleHelper(AzureCreator.newBuilder()
        .setUseVmss(true)
        .setUserAgent(userAgent)
        .setNumberOfVMs(1)
        .setPublicIP(false)
        .build());
  }

  /**
   * Combines many tests by running through the lifecycle (and more) of three VMs using Storage
   * Accounts:
   * 1. allocate --> success (no exceptions)
   * 2. find --> returns VM in list and verifies the VMs are correctly set up
   * 3. getInstanceState --> returns RUNNING state
   * 4. delete --> success (no exceptions)
   * 5. find --> returns empty list
   * 6. getInstanceState --> returns UNKNOWN state
   * 7. delete --> success (no exceptions)
   *
   * @throws Exception
   */
  private void fullCycleHelper(AzureCreator azureCreator) throws Exception {
    LOG.info("0. set up.");

    Map map = azureCreator.createMap();
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), new SimpleConfiguration(map));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the three VMs to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < azureCreator.getNumberOfVMs(); i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // verify that allocate returns a correctly sized list
    LOG.info("1. allocate() all VMs successfully.");
    Collection<? extends AzureComputeInstance<?>> allocatedInstances =
        provider.allocate(template, instanceIds, instanceIds.size());
    assertEquals(instanceIds.size(), allocatedInstances.size());

    // verify that all instances can be found
    LOG.info("2. find() all VMs.");
    Collection<String> allocatedInstanceIds =
        allocatedInstances.stream().map(AzureComputeInstance::getId).collect(Collectors.toList());
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> foundInstances =
        provider.find(template, allocatedInstanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    for (AzureComputeInstance<? extends AzureInstance> instance : foundInstances) {
      verifyAzureInstance(instance, azureCreator);
      AzureInstance vm = instance.unwrap();
      if (vm instanceof VirtualMachine) {
        verifyVM(instance, azureCreator);
      } else if (vm instanceof VirtualMachineScaleSetVM) {
        verifyVM((VirtualMachineScaleSetVM) vm, azureCreator);
      } else {
        fail("Unexpected instance type " + vm.getClass());
      }
    }

    // verify that all instances are in the RUNNING state
    LOG.info("3. getInstanceState() shows all instances as RUNNING.");
    // get the instanceIds from the previous find() call
    Map<String, InstanceState> instanceStates =
        provider.getInstanceState(template, allocatedInstanceIds);
    assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : allocatedInstanceIds) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    LOG.info("4. delete all VMs successfully.");
    if (azureCreator.useVmss()) {
      provider.delete(template, Collections.emptyList());
    } else {
      provider.delete(template, instanceIds);
    }

    // verify that all instances were correctly deleted
    LOG.info("5. find() all the now deleted VMs.");
    assertEquals(0, provider.find(template, allocatedInstanceIds).size());

    // verify that all instances are in the UNKNOWN state
    LOG.info("6. getInstanceState() on all the now deleted VMs.");
    Map<String, InstanceState> instanceStatesDeleted = provider
        .getInstanceState(template, allocatedInstanceIds);
    assertEquals(instanceIds.size(), instanceStatesDeleted.size());
    for (String instance : allocatedInstanceIds) {
      assertEquals(InstanceStatus.UNKNOWN, instanceStatesDeleted.get(instance)
          .getInstanceStatus());
    }

    // verify that deleting non-existent VMs doesn't throw an exception
    LOG.info("7. delete() the already deleted VMs again.");
    if (azureCreator.useVmss()) {
      provider.delete(template, Collections.emptyList());
    } else {
      provider.delete(template, instanceIds);
    }
  }

  /**
   * AzureInstance checks.
   *
   * @param instance the instance to check
   * @param azureCreator used in verification
   */
  private void verifyAzureInstance(
      AzureComputeInstance<? extends AzureInstance> instance,
      AzureCreator azureCreator) {
    if (azureCreator.withPublicIP()) {
      assertThat(instance.unwrap().getPublicIPAddress()).isNotNull();
    }
    assertEquals(instance.unwrap().regionName(), azureCreator.getRegion());
    assertThat(instance.unwrap().name()).isNotNull();
    assertThat(instance.unwrap().computerName()).isNotNull();
    assertTrue(instance.unwrap().size().toString().equalsIgnoreCase(azureCreator.getVmSize()));

    NetworkInterfaceBase networkInterface = instance.unwrap().getPrimaryNetworkInterface();
    assertThat(networkInterface).isNotNull();
    assertEquals(azureCreator.withAcceleratedNetworking(), networkInterface.isAcceleratedNetworkingEnabled());
  }

  /**
   * VirtualMachine checks.
   *
   * @param instance the virtual machine to check
   * @param azureCreator used in verification
   */
  private void verifyVM(
      AzureComputeInstance<? extends AzureInstance> instance,
      AzureCreator azureCreator) {
    VirtualMachine vm = (VirtualMachine) instance.unwrap();

    // networking checks
    String privateFqdn = instance.getProperties().get(AzureComputeInstance
        .AzureComputeInstanceDisplayPropertyToken.PRIVATE_FQDN.unwrap().getDisplayKey());
    if (azureCreator.hasHostFqdnSuffix()) {
      assertTrue(privateFqdn.contains(TestHelper.TEST_HOST_FQDN_SUFFIX));
    }

    // Private IP Address checks
    NetworkInterface ni = vm.getPrimaryNetworkInterface();
    IPAllocationMethod privateIPAllocationMethod = ni.primaryIPConfiguration().inner().privateIPAllocationMethod();
    if (azureCreator.withStaticPrivateIpAddress()) {
      assertTrue(privateIPAllocationMethod.equals(IPAllocationMethod.STATIC));
    } else {
      assertTrue(privateIPAllocationMethod.equals(IPAllocationMethod.DYNAMIC));
    }

    // storage
    String expectedStorageAccountType =
        Configurations.convertStorageAccountTypeString(azureCreator.getStorageAccountType());
    if (azureCreator.withManagedDisks()) {
      // verify that the OS disk has the correct Storage Account Type (e.g. standard / premium)
      String actualStorageAccountType = vm.osDiskStorageAccountType().toString();
      assertEquals(expectedStorageAccountType, actualStorageAccountType);

      // verify that the data disks have the correct Storage Account Type
      for (VirtualMachineDataDisk dataDisk : vm.dataDisks().values()) {
        actualStorageAccountType = dataDisk.storageAccountType().toString();
        assertEquals(expectedStorageAccountType, actualStorageAccountType);
      }
    } else {
      // verify that the Storage Account has the correct Storage Account Type (e.g. standard / premium)
      String saName = getStorageAccountNameFromStorageProfile(vm.storageProfile());
      String actualStorageAccountType = azure.storageAccounts()
          .getByResourceGroup(azureCreator.getComputeResourceGroup(), saName).sku().name()
          .toString();
      assertEquals(expectedStorageAccountType, actualStorageAccountType);

      // verify that the first 3 characters of the Storage Account name match up with the instanceId
      String storageAccountPrefix = saName.substring(0, 3);
      String instanceId = instance.getProperties().get(AzureComputeInstance
          .AzureComputeInstanceDisplayPropertyToken.INSTANCE_ID.unwrap().getDisplayKey());
      String instanceIdPrefix = instanceId.substring(instanceId.length() - 36, instanceId.length()).substring(0, 3);
      assertEquals(storageAccountPrefix, instanceIdPrefix);
    }

    // User Assigned MSI checks
    if (azureCreator.withUserAssignedMsi()) {
      assertTrue(vm.isManagedServiceIdentityEnabled());
      assertTrue(vm.managedServiceIdentityType().equals(ResourceIdentityType.USER_ASSIGNED));
      assertTrue(!vm.userAssignedManagedServiceIdentityIds().isEmpty());

      boolean containsMsi = false;
      for (String i : vm.userAssignedManagedServiceIdentityIds()) {
        if (i.contains(TestHelper.TEST_USER_ASSIGNED_MSI_NAME)) {
          containsMsi = true;
        }
      }
      assertTrue(containsMsi);
    }
  }

  /**
   * Check VirtualMachineScaleSetVM.
   *
   * @param vm the VMSS VM to check
   * @param azureCreator used in verification
   */
  private void verifyVM(
      VirtualMachineScaleSetVM vm,
      AzureCreator azureCreator) {
    // verify that the OS disk has the correct Storage Account Type (e.g. standard / premium)
    String actualStorageAccountType = vm.storageProfile().osDisk().managedDisk().storageAccountType().toString();
    assertEquals(azureCreator.getStorageAccountType(), actualStorageAccountType);

    // verify that the data disks have the correct Storage Account Type
    for (VirtualMachineDataDisk dataDisk : vm.dataDisks().values()) {
      actualStorageAccountType = dataDisk.storageAccountType().toString();
      assertEquals(azureCreator.getStorageAccountType(), actualStorageAccountType);
    }

    // User Assigned MSI checks
    if (azureCreator.withUserAssignedMsi()) {
      VirtualMachineScaleSet vmss = azure.virtualMachineScaleSets()
          .getByResourceGroup(azureCreator.getComputeResourceGroup(), azureCreator.getGroupId());
      assertTrue(vmss.isManagedServiceIdentityEnabled());
      assertTrue(vmss.managedServiceIdentityType().equals(ResourceIdentityType.USER_ASSIGNED));
      assertTrue(!vmss.userAssignedManagedServiceIdentityIds().isEmpty());

      boolean containsMsi = false;
      for (String i : vmss.userAssignedManagedServiceIdentityIds()) {
        if (i.contains(TestHelper.TEST_USER_ASSIGNED_MSI_NAME)) {
          containsMsi = true;
        }
      }
      assertTrue(containsMsi);
    }
  }

  @Test
  public void vmFindWithAzureErrorInvalidCredentialsThrows() throws Exception {
    LOG.info("vmFindWithAzureErrorInvalidCredentialsReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // set up the custom plugin config
    File configurationDirectory = temporaryFolder.getRoot();
    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));
    printWriter.println("azure-validate-credentials: false");
    printWriter.close();

    // set up the custom config with bad credentials
    TestHelper.setAzurePluginConfigNull();
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
        "fake-client-secret");

    Launcher launcher = new AzureLauncher();
    launcher.initialize(configurationDirectory, null); // so we bring in the custom config
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    try {
      provider.find(template, instanceIds);
      Assert.fail("find is expected to fail");
    } catch (Exception expected) {
    }
  }

  @Test
  public void vmFindWithPartiallyDeletedVirtualMachine() throws Exception {
    LOG.info("vmFindWithPartiallyDeletedVirtualMachine");

    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate the VM the first time
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. verify that the instance can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> foundInstances =
        provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    // 3. verify that the instance is in the RUNNING state
    LOG.info("3. getInstanceState");
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 4. find and getInstanceState with partially deleted vm
    LOG.info("4. find and getInstanceState again with partially deleted vm");
    AzureComputeInstance<? extends AzureInstance> toDelete = foundInstances.iterator().next();
    azure.virtualMachines().deleteById(
        ((com.cloudera.director.azure.compute.instance.VirtualMachine) toDelete.unwrap()).id());

    Collection<? extends AzureComputeInstance<? extends AzureInstance>> actual = provider
        .find(template, Collections.singletonList(toDelete.getId()));
    assertEquals(1, actual.size());
    Assert.assertNull(actual.iterator().next().unwrap());

    Map<String, InstanceState> actualState = provider
        .getInstanceState(template, Collections.singletonList(toDelete.getId()));
    assertEquals(1, actualState.size());
    assertEquals(InstanceStatus.FAILED, actualState.get(toDelete.getId()).getInstanceStatus());

    // 5. delete
    LOG.info("5. delete");
    provider.delete(template, instanceIds);
  }

  @Test
  public void vmFindWithNonexistentResourceGroupReturnsEmptyList() throws Exception {
    LOG.info("vmFindWithNonexistentResourceGroupReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void vmssFindWithNonexistentResourceGroupReturnsEmptyList() throws Exception {
    LOG.info("vmssFindWithNonexistentResourceGroupReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");
    map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void vmFindWithNonexistentVirtualMachineReturnsEmptyList() throws Exception {
    LOG.info("vmFindWithNonexistentVirtualMachineReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void vmssFindWithNonexistentVirtualMachineScaleGroupReturnsEmptyList() throws Exception {
    LOG.info("vmssFindWithNonexistentVirtualMachineScaleGroupReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void vmGetInstanceStateWithInvalidCredentialsReturnsMapOfUnknowns() throws Exception {
    LOG.info("vmGetInstanceStateWithInvalidCredentialsReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // set up the custom plugin config
    File configurationDirectory = temporaryFolder.getRoot();
    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));
    printWriter.println("azure-validate-credentials: false");
    printWriter.close();

    // set up the custom config with bad credentials
    TestHelper.setAzurePluginConfigNull();
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
        "fake-client-secret");

    Launcher launcher = new AzureLauncher();
    launcher.initialize(configurationDirectory, null); // so we bring in the custom config
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void vmGetInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns() throws Exception {
    LOG.info("vmGetInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void vmssGetInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns() throws Exception {
    LOG.info("vmssGetInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");
    map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void vmGetInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns() throws Exception {
    LOG.info("vmGetInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void vmssGetInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns() throws Exception {
    LOG.info("vmssGetInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  /**
   * Verifies that on delete() we delete the public IP of the VM only if the template specified that
   * there was one (e.g. we won't delete a public IP that was manually attached):
   * 1. allocate the VM with a public IP
   * 2. cache the public IP's id to use for clean up later
   * 3. delete with a different template that specifies public IP = "No"
   * 4. find to verify that all instances were correctly deleted
   * 5. verify the public IP still exists
   * 6. cleanup: delete the public IP
   * 7. cleanup: verify the public IP was deleted
   *
   * @throws Exception
   */
  @Test
  public void deleteWithManuallyAttachedPublicIpExpectPublicIpDoesNotGetDeleted() throws Exception {
    LOG.info("deleteWithManuallyAttachedPublicIpExpectPublicIpDoesNotGetDeleted");

    // set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // 1. allocate the VM with a public IP
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. cache the public IP's id to use for clean up later
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> foundInstances = provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());
    VirtualMachine vm = (VirtualMachine) foundInstances.iterator().next().unwrap(); // there's only one
    String publicIpId = vm.getPrimaryPublicIPAddressId();

    // 3. delete with a different template that specifies public IP = "No"
    LOG.info("2. delete");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey(),
        "No");
    AzureComputeInstanceTemplate templateNoPublicIp = new AzureComputeInstanceTemplate(
        TEMPLATE_NAME, new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);
    provider.delete(templateNoPublicIp, instanceIds);

    // 4. find to verify that all instances were correctly deleted
    LOG.info("3. find");
    assertEquals(0, provider.find(templateNoPublicIp, instanceIds).size());

    // 5. verify the public IP still exists
    Assert.assertNotNull(azure.publicIPAddresses().getById(publicIpId));

    // 6. cleanup: delete the public IP
    azure.publicIPAddresses().deleteById(publicIpId);

    // 7. cleanup: verify the public IP was deleted
    Assert.assertNull(azure.publicIPAddresses().getById(publicIpId));
  }

  /**
   * Verifies that find(), getInstanceState(), and delete() work when the VM and some of it's resources are already
   * deleted by doing the following:
   * - allocate 3x Virtual Machines with Managed Disks, Network Interfaces, and Public IPs
   * - manually delete all Virtual Machines and then follow this truth table for deleting one of either their attached
   *   Storage, Network Interfaces or Public IPs.
   *   Columns are: delete Storage | delete Network Interface | delete Public IP
   *     VM with MD 1: y n n
   *     VM with MD 2: n y n
   *     VM with MD 3: n n y
   * -  delete the VMs by calling delete()
   * -  verify that everything was deleted; if not call delete again and repeat up to 3 times
   *
   * @throws Exception
   */
  @Test
  public void fullCycleVMManagedIsRetryable()
      throws Exception {
    fullCycleRetryableHelper(AzureCreator.newBuilder()
        .setManagedDisks(true)
        .setNumberOfVMs(3)
        .setDataDiskCount(3)
        .build());  }

  /**
   * Verifies that find(), getInstanceState(), and delete() work when the VM and some of it's resources are already
   * deleted by doing the following:
   * - allocate 3x Virtual Machines with Storage Accounts, Network Interfaces, and Public IPs
   * - manually delete all Virtual Machines and then follow this truth table for deleting one of either their attached
   *   Storage, Network Interfaces or Public IPs.
   *   Columns are: delete Storage | delete Network Interface | delete Public IP
   *     VM with MD 1: y n n
   *     VM with MD 2: n y n
   *     VM with MD 3: n n y

   * -  delete the VMs by calling delete()
   * -  verify that everything was deleted; if not call delete again and repeat up to 3 times
   *
   * @throws Exception
   */
  @Test
  public void fullCycleVMUnmanagedIsRetryable()
      throws Exception {
    fullCycleRetryableHelper(AzureCreator.newBuilder()
        .setManagedDisks(false)
        .setNumberOfVMs(3)
        .build());
  }

  private void fullCycleRetryableHelper(AzureCreator azureCreator) throws Exception {
    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();

    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE.unwrap().getConfigKey(),
        TestHelper.TEST_DATA_DISK_SIZE);
    if (azureCreator.withManagedDisks()) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(), "Yes");
    } else {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(), "No");
    }
    map.remove(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap().getConfigKey());
    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap().getConfigKey(),
        SkuName.STANDARD_LRS.toString());

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the three VMs to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < azureCreator.getNumberOfVMs(); i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // 1. allocate
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. find
    // verify that all instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> foundInstances = provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    // 3. getInstanceState
    // verify that all instances are in the RUNNING state
    LOG.info("3. getInstanceState");
    // get the instanceIds from the previous find() call
    Collection<String> foundInstanceIds = new ArrayList<>();
    for (AzureComputeInstance<? extends AzureInstance> instance : foundInstances) {
      foundInstanceIds.add(instance.getId());
    }
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, foundInstanceIds);
    assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 4. manually delete all the Virtual Machines only
    LOG.info("4. manually delete the virtual machines only (takes a few minutes)");
    String rgName = azureCreator.getComputeResourceGroup();
    String prefix = azureCreator.getInstanceNamePrefix();

    for (String id : instanceIds) {
      azure.virtualMachines().deleteByResourceGroup(rgName, getVmName(id, prefix));
      LOG.info("Deleted VM {}", getVmName(id, prefix));
    }

    // 5. manually delete the rest of the resources following the truth table in this method's javadocs
    LOG.info("5. manually delete other resources following the table in the method's javadoc");
    int i = 0;
    for (String id : instanceIds) {
      // what to delete logic
      boolean deleteStorage = i == 0;
      boolean deleteNic = i == 1;
      boolean deletePip = i == 2;
      i += 1;

      String commonResourceNamePrefix = getFirstGroupOfUuid(id);

      // delete storage (if managed delete all but one of the disks)
      if (deleteStorage) {
        if (azureCreator.withManagedDisks()) {
          azure.disks()
              .deleteByResourceGroup(rgName, commonResourceNamePrefix + MANAGED_OS_DISK_SUFFIX);
          LOG.info("Deleted disk {}", commonResourceNamePrefix + MANAGED_OS_DISK_SUFFIX);

          int numberOfManagedDisks = azureCreator.getDataDiskCount();
          // leave one disk behind
          for (int j = 0; j < numberOfManagedDisks - 1; j += 1) {
            azure.disks().deleteByResourceGroup(rgName, commonResourceNamePrefix + "-" + j);
            LOG.info("Deleted disk {}", commonResourceNamePrefix + "-" + j);

          }
        } else {
          azure.storageAccounts().deleteByResourceGroup(rgName, commonResourceNamePrefix);
          LOG.info("Deleted storage account {}", commonResourceNamePrefix);

        }
      }

      // delete nic
      if (deleteNic) {
        azure.networkInterfaces().deleteByResourceGroup(rgName, commonResourceNamePrefix);
        LOG.info("Deleted nic {}", commonResourceNamePrefix);
      }

      // delete pip (must delete the nic first)
      if (deletePip) {
        azure.networkInterfaces().deleteByResourceGroup(rgName, commonResourceNamePrefix);
        azure.publicIPAddresses().deleteByResourceGroup(rgName, commonResourceNamePrefix);
        LOG.info("Deleted nic {} and public IP {}", commonResourceNamePrefix, commonResourceNamePrefix);
      }
    }

    // 6. find() with some resources missing - verify that all VMs can be found
    LOG.info("6. find");
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> partiallyDeletedFoundInstances =
        provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), partiallyDeletedFoundInstances.size());

    // 7. getInstanceState() with some resources missing - verify that all VMs are in the FAILED state
    LOG.info("7. getInstanceState");
    // get the instanceIds from the previous find() call
    Collection<String> partiallyDeletedFoundInstanceIds = new ArrayList<>();
    for (AzureComputeInstance instance : partiallyDeletedFoundInstances) {
      partiallyDeletedFoundInstanceIds.add(instance.getId());
    }
    Map<String, InstanceState> partiallyDeletedInstanceStates =
        provider.getInstanceState(template, partiallyDeletedFoundInstanceIds);
    assertEquals(instanceIds.size(), partiallyDeletedInstanceStates.size());
    for (String instanceId : instanceIds) {
      LOG.info("Status for instance id {}: {}.",
          instanceId, partiallyDeletedInstanceStates.get(instanceId).getInstanceStatus());
      assertEquals(InstanceStatus.FAILED, partiallyDeletedInstanceStates.get(instanceId).getInstanceStatus());
    }

    // 8. delete() with some resources missing, up to 3 times
    LOG.info("8. delete");
    int attempt = 0;
    int maxAttempts = 3;
    while (attempt < maxAttempts) {
      LOG.info("delete() attempt {} of {}.", attempt, maxAttempts);

      // delete()
      try {
        provider.delete(template, instanceIds);
      } catch (UnrecoverableProviderException e) {
        LOG.error("delete() threw UnrecoverableProviderException number {}. Error: ", attempt, e);
        attempt += 1;
        continue;
      } catch (Exception e) {
        LOG.error("A generic exception was thrown when calling delete() (this should not happen). Error: ", e);
        Assert.fail();
        break;
      }

      // verify everything has been deleted
      if (AzureComputeProviderLiveTestHelper.resourcesDeleted(azure, map, instanceIds)) {
        break;
      } else {
        attempt += 1;
      }
    }

    assertTrue(
        String.format("delete() failed to delete all resources within %s tries.", maxAttempts),
        attempt < maxAttempts);
  }

  @Ignore
  @Test
  public void allocateWithTheSameInstanceIdMultipleTimesExpectIdempotency() throws Exception {
    LOG.info("allocateWithTheSameInstanceIdMultipleTimesExpectIdempotency");

    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate the VM the first time
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. verify that the instance can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    // 3. verify that the instance is in the RUNNING state
    LOG.info("3. getInstanceState");
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 4. try to create the same VM again - it won't create one because it already exists in the RG
    // and will throw an UnrecoverableProviderException since it didn't create at least minCount VMs
    LOG.info("4. allocate (again)");
    boolean hitUnrecoverableProviderException = false;
    try {
      provider.allocate(template, instanceIds, instanceIds.size());
    } catch (UnrecoverableProviderException e) {
      hitUnrecoverableProviderException = true;
    }

    // 5. verify that an UnrecoverableProviderException was thrown and caught
    LOG.info("5. verify UnrecoverableProviderException");
    assertTrue(hitUnrecoverableProviderException);

    // 6. verify that the instance can still be found
    LOG.info("6. find");
    foundInstances = provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    // 7. verify that the instance is still in the RUNNING state
    LOG.info("7. getInstanceState");
    instanceStates = provider.getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 8. delete the vm
    LOG.info("8. delete");
    provider.delete(template, instanceIds);

    // 9. verify that all the instances were correctly deleted
    LOG.info("9. find");
    assertEquals(0, provider.find(template, instanceIds).size());

    // 10. verify that all instances are in the UNKNOWN state
    LOG.info("10. getInstanceState");
    Map<String, InstanceState> instanceStatesDeleted = provider
        .getInstanceState(template, instanceIds);
    assertEquals(instanceIds.size(), instanceStatesDeleted.size());
    for (String instance : instanceIds) {
      assertEquals(InstanceStatus.UNKNOWN, instanceStatesDeleted.get(instance)
          .getInstanceStatus());
    }

    // 11. try to delete the VM again - it won't because the VM isn't in the RG
    LOG.info("11. delete (again)");
    provider.delete(template, instanceIds);
  }

  /**
   * This test tries to allocate two instances with the same instanceIds at the same time. The
   * outcome is that no VMs are allocated and no resources are leaked. Director ensures allocate
   * will never be called with duplicate instanceIds. The goal of this test is to make sure nothing
   * is leaked and that the behavior of allocate with duplicate instanceIds stays consistent.
   *
   * @throws Exception
   */
  @Ignore // allocate will create one instance and find will find it twice
  @Test
  public void allocateWithTwoIdenticalInstanceIdsAtTheSameTimeExpectNoLeakedResources()
      throws Exception {
    LOG.info("allocateWithTwoIdenticalInstanceIdsAtTheSameTimeExpectNoLeakedResources");

    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the repeated instanceId to use for this test
    String instanceId = UUID.randomUUID().toString();
    Collection<String> doubleInstanceIds = new ArrayList<>();
    doubleInstanceIds.add(instanceId);
    doubleInstanceIds.add(instanceId);
    Collection<String> singleInstanceIds = new ArrayList<>();
    singleInstanceIds.add(instanceId);

    // 1. allocate the VMs, allow success even if none come up
    LOG.info("1. allocate");
    provider.allocate(template, doubleInstanceIds, 0);

    // 2. verify that neither instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, singleInstanceIds);
    assertEquals(foundInstances.size(), 0);
  }

  /**
   * This tests that using a VM image that does not have a purchase plan attached works by deploying
   * the official RHEL 6.7 OS image (the RHEL images do not have purchase plans).
   *
   * @throws Exception
   */
  @Test
  public void vmAllocateWithImageThatHasNoPurchasePlanExpectSuccess() throws Exception {
    LOG.info("vmAllocateWithImageThatHasNoPurchasePlanExpectSuccess");

    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        TestHelper.TEST_RHEL_IMAGE_NAME);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. find
    // verify that all instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, instanceIds);
    assertEquals(instanceIds.size(), foundInstances.size());

    // 3. delete
    provider.delete(template, instanceIds);
  }

  @Test
  public void vmssAllocateWithImageThatHasNoPurchasePlanExpectSuccess() throws Exception {
    LOG.info("vmssAllocateWithImageThatHasNoPurchasePlanExpectSuccess");

    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        TestHelper.TEST_RHEL_IMAGE_NAME);
    map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. find
    // verify that all instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, Collections.emptyList());
    assertEquals(instanceIds.size(), foundInstances.size());

    // 3. delete
    provider.delete(template, Collections.emptyList());
  }

  @Ignore("https://jira.cloudera.com/browse/DIR-8188")
  @Test
  public void allocateVmWithCustomImageWithPlanExpectSuccess() throws Exception {
    LOG.info("allocateVmWithCustomImageWithPlanExpectSuccess");
    allocateVmWithCustomImageHelper(true);
  }

  @Test
  public void allocateVmWithCustomImageWithoutPlanExpectSuccess() throws Exception {
    LOG.info("allocateVmWithCustomImageWithoutPlanExpectSuccess");
    allocateVmWithCustomImageHelper(false);
  }

  /**
   * Helper class to make custom image and deploy VM with it
   *
   * @param withPlan true if create custom image with purchase plan
   * @throws Exception
   */
  private void allocateVmWithCustomImageHelper(boolean withPlan) throws Exception {
    LOG.info("Part 1 of 2: Build the Custom Managed VM Image.");
    LOG.info("0. create custom image.");
    String imageUri = CustomVmImageTestHelper.buildCustomManagedVmImage(
        "test-image-" + UUID.randomUUID(), withPlan);
    LOG.info("Custom Image URI: {}", imageUri);

    LOG.info("Part 2 of 2: Allocate the VM with a Custom Image.");
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        imageUri);
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    if (withPlan) {
      cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
          .getConfigKey(), "/publisher/cloudera/product/cloudera-centos-os/name/6_7");
    }

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(cfgMap), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate the VM the first time
    LOG.info("1. allocate vm with custom image");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. verify that neither instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<?>> foundInstances = provider.find(template, instanceIds);
    assertEquals(1, foundInstances.size());

    // 3. delete VM
    LOG.info("3. delete VM");
    provider.delete(template, instanceIds);

    // 4. allocate VMSS with the same image
    cfgMap.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
    cfgMap.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));
    AzureComputeInstanceTemplate vmssTemplate = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(cfgMap), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    LOG.info("4. allocate vmss with custom image");
    provider.allocate(vmssTemplate, instanceIds, instanceIds.size());

    // 5. verify that all instances can be found
    LOG.info("5. find");
    foundInstances = provider.find(vmssTemplate, Collections.emptyList());
    assertEquals(instanceIds.size(), foundInstances.size());

    // 6. delete VMSS
    LOG.info("6. delete vmss");
    provider.delete(vmssTemplate, Collections.emptyList());

    // 7. delete custom image
    LOG.info("7. delete custom image");
    CustomVmImageTestHelper.deleteCustomManagedVmImage(imageUri);
  }

  @Test
  public void createVmWithImplicitMsiAndAddToExistingAadGroup()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithImplicitMsiAndAddToExistingAadGroup");
    msiTestHelper(false, true, true, false);
  }

  @Test
  public void createVmWithImplicitMsiAndNotAddToExistingAadGroup()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithImplicitMsiAndNotAddToExistingAadGroup");
    msiTestHelper(false, true, false, false);
  }

  @Test
  public void createVmWithUserAssignedMsiAndSystemAssignedMsi()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("fullCycleWithUserAssignedMsiAndSystemAssignedMsi");
    msiTestHelper(false, true, true, true);
  }

  @Test
  public void createVmWithoutImplicitMsi()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithoutImplicitMsi");
    msiTestHelper(false, false, false, false);
  }

  @Test
  public void createVmssWithoutImplicitMsi()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmssWithoutImplicitMsi");
    msiTestHelper(true, false, false, false);
  }

  /**
   * Helper function to run implicit MSI tests
   *
   * @param withImplicitMsi true for creating VM with implicit MSI
   * @param assignGroup true for adding implicit MSI to existing AAD group
   * @param withUserAssignedMsi true for creating VM with user assigned MSI
   * @throws Exception
   */
  private void msiTestHelper(
      boolean isScaleSet, boolean withImplicitMsi, boolean assignGroup, boolean withUserAssignedMsi)
      throws Exception {
    // 0. set up
    LOG.info("0. set up");
    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), withImplicitMsi ? "Yes" : "No");

    if (assignGroup) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
              .unwrap().getConfigKey(),
          TestHelper.TEST_AAD_GROUP_NAME);
    }

    if (withUserAssignedMsi) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME.unwrap().getConfigKey(),
          TestHelper.TEST_USER_ASSIGNED_MSI_NAME);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP.unwrap().getConfigKey(),
          TestHelper.TEST_RESOURCE_GROUP);
    }

    if (isScaleSet) {
      map.put(GROUP_ID.unwrap().getConfigKey(), RandomStringUtils.randomAlphabetic(8).toLowerCase());
      map.put(AUTOMATIC.unwrap().getConfigKey(), String.valueOf(true));
    }

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the instanceId to use for this test
    String instanceId = UUID.randomUUID().toString();
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(instanceId);

    // 1. allocate the VMs, allow success even if none come up
    LOG.info("1. allocate");
    List<String> instances = provider
        .allocate(template, instanceIds, 1)
        .stream()
        .map(AzureComputeInstance::getId)
        .collect(Collectors.toList());

    // 2. verify instances can be found
    LOG.info("2. find");
    ArrayList<AzureComputeInstance<? extends AzureInstance>> foundInstances =
        (ArrayList<AzureComputeInstance<? extends AzureInstance>>) provider.find(template, instances);
    assertEquals(foundInstances.size(), 1);

    // 3. verify uaMSI and saMSI
    LOG.info("3. MSI");
    GraphRbacManager graphRbacManager = TestHelper.getAzureCredentials().getGraphRbacManager();
    String saMsiObjectId = null;

    if (withUserAssignedMsi) {
      // uaMSI checks
      if (!isScaleSet) {
        for (AzureComputeInstance instance : foundInstances) {
          VirtualMachine vm = (VirtualMachine) instance.unwrap();

          // the saMSI object should be null
          saMsiObjectId = vm.systemAssignedManagedServiceIdentityPrincipalId();
          assertThat(saMsiObjectId).isNull();
          assertTrue(vm.isManagedServiceIdentityEnabled());
          assertTrue(
              vm.managedServiceIdentityType().equals(ResourceIdentityType.USER_ASSIGNED));
          assertTrue(!vm.userAssignedManagedServiceIdentityIds().isEmpty());

          boolean containsMsi = false;
          for (String i : vm.userAssignedManagedServiceIdentityIds()) {
            if (i.contains(TestHelper.TEST_USER_ASSIGNED_MSI_NAME)) {
              containsMsi = true;
            }
          }
          assertTrue(containsMsi);
        }
      } else {
        VirtualMachineScaleSet vmss = azure.virtualMachineScaleSets()
            .getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, template.getGroupId());
        saMsiObjectId = vmss.systemAssignedManagedServiceIdentityPrincipalId();
        assertThat(saMsiObjectId).isNull();
        assertThat(vmss.isManagedServiceIdentityEnabled());
        assertThat(vmss.managedServiceIdentityType()).isEqualTo(ResourceIdentityType.USER_ASSIGNED);
        assertThat(vmss.userAssignedManagedServiceIdentityIds().isEmpty()).isFalse();
        assertThat(vmss.userAssignedManagedServiceIdentityIds().stream()
            .anyMatch(si -> si.contains(TestHelper.TEST_USER_ASSIGNED_MSI_NAME))).isTrue();
      }
    }

    // verify saMSI is created and present in group
    if (withImplicitMsi) {
      if (!isScaleSet) {
        VirtualMachine vm = (VirtualMachine) foundInstances.get(0).unwrap();
        saMsiObjectId = vm.systemAssignedManagedServiceIdentityPrincipalId();
      } else {
        VirtualMachineScaleSet vmss = azure.virtualMachineScaleSets()
            .getByResourceGroup(TestHelper.TEST_RESOURCE_GROUP, template.getGroupId());
        saMsiObjectId = vmss.systemAssignedManagedServiceIdentityPrincipalId();
      }

      // FIXME getById() throws an exception if service principal does not exist.
      // This is is an Azure Java SDK issue: https://github.com/Azure/azure-sdk-for-java/issues/1845
      ServicePrincipal msiObject;
      try {
        msiObject = graphRbacManager.servicePrincipals().getById(saMsiObjectId);
      } catch (IllegalArgumentException e) {
        msiObject = null;
      }

      if (withUserAssignedMsi) {
        Assert.assertNull(saMsiObjectId);
        Assert.assertNull(msiObject);
      } else {
        Assert.assertNotNull(msiObject); // failing on VMSS here

        Set<ActiveDirectoryObject> members =
            graphRbacManager.groups().getByName(TestHelper.TEST_AAD_GROUP_NAME).listMembers();
        boolean found = false;
        for (ActiveDirectoryObject object : members) {
          if (object.id().equals(saMsiObjectId)) {
            LOG.info("Found implicit MSI: name {}, id {}.", object.name(), object.id());
            found = true;
            break;
          }
        }
        if (assignGroup) {
          assertTrue(found);
        }
      }
    }

    // 4. delete vm
    LOG.info("4. delete VM");
    if (isScaleSet) {
      provider.delete(template, Collections.emptyList());
    } else {
      provider.delete(template, instanceIds);
    }

    // 5. confirm vm deletion
    LOG.info("5. confirm vm deletion");
    assertEquals(0, provider.find(template, instances).size());

    // wait a little for AAD to update
    Thread.sleep(10000);

    // 6. verify implicit MSI is removed (from AAD and from group)
    if (assignGroup) {
      LOG.info("6. verify implicit MSI deletion");
      Set<ActiveDirectoryObject> members =
          graphRbacManager.groups().getByName(TestHelper.TEST_AAD_GROUP_NAME).listMembers();
      boolean found = false;
      for (ActiveDirectoryObject object : members) {
        if (object.id().equals(saMsiObjectId)) {
          found = true;
          break;
        }
      }
      Assert.assertFalse(found);
    }
    // FIXME getById() throws an exception if service principal does not exist.
    // This is is an Azure Java SDK issue: https://github.com/Azure/azure-sdk-for-java/issues/1845
    try {
      graphRbacManager.servicePrincipals().getById(saMsiObjectId);
    } catch (IllegalArgumentException e) {
      LOG.error("Expected exception: {}", e.getMessage());
    }
  }
}
