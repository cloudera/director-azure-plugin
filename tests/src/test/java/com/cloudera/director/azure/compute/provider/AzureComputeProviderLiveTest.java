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

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.CustomVmImageTestHelper;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.ActiveDirectoryObject;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.ServicePrincipal;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.Launcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
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
public class AzureComputeProviderLiveTest {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProviderLiveTest.class);

  // Fields used by checks
  private static AzureCredentials credentials;
  private static Azure azure;

  private static final String TEMPLATE_NAME = "LiveTestInstanceTemplate";
  private static final Map<String, String> TAGS = TestHelper.buildTagMap();

  private static final DefaultLocalizationContext DEFAULT_LOCALIZATION_CONTEXT =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void createLiveTestResources() throws Exception {
    LOG.info("createLiveTestResources");

    Assume.assumeTrue(TestHelper.runLiveTests());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf

    // initialize everything only if live check passes
    credentials = TestHelper.getAzureCredentials();
    azure = credentials.authenticate();
    TestHelper.buildLiveTestEnvironment(azure);
  }

  @After
  public void reset() throws Exception {
    LOG.info("reset");

    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }

  @AfterClass
  public static void destroyLiveTestResources() throws Exception {
    LOG.info("destroyLiveTestResources");
    // this method is always called

    // destroy everything only if live check passes
    if (TestHelper.runLiveTests()) {
      TestHelper.destroyLiveTestEnvironment(azure);
    }
  }

  /**
   * Basic pre-commit full cycle sanity live test. Similar to the other fullCycle tests but left
   * standalone for ease of remembering which test to run before committing.
   *
   * @throws Exception
   */
  @Test
  public void fullCycle() throws Exception {
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 3, false, false, null);
  }

  @Test
  public void fullCycleWithValidStorageExpectSuccess() throws Exception {
    // Managed Disks + Premium Storage
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "4095", 1, false, false, null);
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "2048", 1, false, false, null);
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "1024", 1, false, false, null);
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "1023", 1, false, false, null);
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "512", 1, false, false, null);

    // Storage Accounts + Premium Storage
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "4095", 1, false, false, null);
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "2048", 1, false, false, null);
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "1024", 1, false, false, null);
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "1023", 1, false, false, null);
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "512", 1, false, false, null);

    // Managed Disks + Standard Storage
    fullCycleHelper(true, SkuName.STANDARD_LRS.toString(), "4095", 1, false, false, null);

    // Storage Accounts + Standard Storage
    fullCycleHelper(false, SkuName.STANDARD_LRS.toString(), "4095", 1, false, false, null);
  }

  @Test
  public void fullCycleWithInvalidStorageExpectException() throws Exception {
    // Managed Disks + Premium Storage
    boolean mdpsExceptionThrown = false;
    try {
      fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), "4096", 1, false, false, null);
    } catch (UnrecoverableProviderException e) {
      mdpsExceptionThrown = true;
    }

    // Storage Accounts + Premium Storage
    boolean sapsExceptionThrown = false;
    try {
      fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), "4096", 1, false, false, null);
    } catch (UnrecoverableProviderException e) {
      sapsExceptionThrown = true;
    }

    // Managed Disks + Standard Storage
    boolean mdssExceptionThrown = false;
    try {
      fullCycleHelper(true, SkuName.STANDARD_LRS.toString(), "4096", 1, false, false, null);
    } catch (UnrecoverableProviderException e) {
      mdssExceptionThrown = true;
    }

    // Storage Accounts + Standard Storage
    boolean sassExceptionThrown = false;
    try {
      fullCycleHelper(false, SkuName.STANDARD_LRS.toString(), "4096", 1, false, false, null);
    } catch (UnrecoverableProviderException e) {
      sassExceptionThrown = true;
    }

    // verify that all exceptions were thrown
    Assert.assertTrue("Managed Disks + Premium Storage Failure", mdpsExceptionThrown);
    Assert.assertTrue("Storage Accounts + Premium Storage Failure", sapsExceptionThrown);
    Assert.assertTrue("Managed Disks + Standard Storage Failure", mdssExceptionThrown);
    Assert.assertTrue("Storage Accounts + Standard Storage Failure", sassExceptionThrown);
  }

  @Test
  public void fullCycleWithoutAvailabilitySetExpectSuccess() throws Exception {
    // Managed Disks + null AS
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 3, true, false, null);

    // Storage Accounts + null AS
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 3, true, false, null);

    // Managed Disks + "" (empty string) AS
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 3, false, true, null);

    // Storage Accounts + "" (empty string) AS
    fullCycleHelper(false, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 3, false, true, null);
  }

  @Test
  public void fullCycleWithDeprecatedStorageAccountTypeStringsExpectSuccess() throws Exception {
    // both Managed Disks and Storage Accounts need to be tested as Managed Disks will default to
    // Premium_LRS if the disk type is invalid, where Storage Accounts will throw:
    // java.lang.IllegalArgumentException: sku.name is required and cannot be null.

    // with Managed Disks
    for (String type : Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()) {
      fullCycleHelper(true, type, TestHelper.TEST_DATA_DISK_SIZE, 1, false, false, null);
    }

    // with Storage Accounts
    for (String type : Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()) {
      fullCycleHelper(false, type, TestHelper.TEST_DATA_DISK_SIZE, 1, false, false, null);
    }
  }

  @Test
  public void fullCycleWithVmImageOneLineExpectSuccess() throws Exception {
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 1, false, false,
        "/publisher/cloudera/offer/cloudera-centos-os/sku/7_2/version/latest");
  }

  @Test
  public void fullCycleWithPreviewVmImageOneLineExpectSuccess() throws Exception {
    // N.b. this test will only pass in subscriptions that have access to the staged image
    fullCycleHelper(true, SkuName.PREMIUM_LRS.toString(), TestHelper.TEST_DATA_DISK_SIZE, 1, false, false,
        "/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/latest");
  }

  /**
   * Combines many tests by running through the lifecycle (and more) of three VMs using Storage
   * Accounts:
   * 1. allocate --> success (no exceptions)
   * 2. find --> returns VM in list; verify that SA name prefixes matches their VM ids if applicable
   * 3. getInstanceState --> returns RUNNING state
   * 4. delete --> success (no exceptions)
   * 5. find --> returns empty list
   * 6. getInstanceState --> returns UNKNOWN state
   * 7. delete --> success (no exceptions)
   *
   * @throws Exception
   */
  private void fullCycleHelper(boolean managed, String storageAccountType, String dataDiskSize, int numberOfVms,
      boolean withNullAvailabilitySet, boolean withEmptyStringAvailabilitySet, String withOneLineImage)
      throws Exception {
    // 0. set up
    LOG.info("0. set up");

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();

    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE.unwrap()
        .getConfigKey(), dataDiskSize);

    if (managed) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_MANAGED);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
          "Yes");
    } else {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
      map.put(
          AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
          "No");
    }

    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap().getConfigKey(),
        storageAccountType);

    if (withNullAvailabilitySet) {
      map.remove(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey());
    }
    if (withEmptyStringAvailabilitySet) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
          .getConfigKey(), "");
    }

    if (withOneLineImage != null) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(), withOneLineImage);
    }

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the three VMs to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    for (int i = 0; i < numberOfVms; i++) {
      instanceIds.add(UUID.randomUUID().toString());
    }
    LOG.info("Using these UUIDs: {}", Arrays.toString(instanceIds.toArray()));

    // 1. allocate
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. find
    // verify that all instances can be found
    LOG.info("2. find");
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), foundInstances.size());

    // Storage Account specific checks
    if (!managed) {
      // verify that the first 3 characters of the Storage Account name match up with the instanceId
      for (AzureComputeInstance instance : foundInstances) {
        String storageAccountPrefix = AzureComputeProvider
            .getStorageAccountNameFromVM(instance.getInstanceDetails()).substring(0, 3);
        String instanceId = instance.getProperties().get(AzureComputeInstance
            .AzureComputeInstanceDisplayPropertyToken.INSTANCE_ID.unwrap().getDisplayKey());
        String instanceIdPrefix =
            instanceId.substring(instanceId.length() - 36, instanceId.length()).substring(0, 3);

        Assert.assertTrue(storageAccountPrefix.equals(instanceIdPrefix));
      }
    }

    // 3. getInstanceState
    // verify that all instances are in the RUNNING state
    LOG.info("3. getInstanceState");
    // get the instanceIds from the previous find() call
    Collection<String> foundInstanceIds = new ArrayList<>();
    for (AzureComputeInstance instance : foundInstances) {
      foundInstanceIds.add(instance.getId());
    }
    Map<String, InstanceState> instanceStates =
        provider.getInstanceState(template, foundInstanceIds);
    Assert.assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      Assert.assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 4. delete
    LOG.info("4. delete");
    provider.delete(template, instanceIds);

    // 5. find
    // verify that all instances were correctly deleted
    LOG.info("5. find");
    Assert.assertEquals(0, provider.find(template, instanceIds).size());

    // 6. getInstanceState
    // verify that all instances are in the UNKNOWN state
    LOG.info("6. getInstanceState");
    Map<String, InstanceState> instanceStatesDeleted = provider
        .getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), instanceStatesDeleted.size());
    for (String instance : instanceIds) {
      Assert.assertEquals(InstanceStatus.UNKNOWN, instanceStatesDeleted.get(instance)
          .getInstanceStatus());
    }

    // 7. delete
    // verify that deleting non-existent VMs doesn't throw an exception
    LOG.info("7. delete");
    provider.delete(template, instanceIds);
  }

  @Test
  public void findWithAzureErrorInvalidCredentialsReturnsEmptyList() throws Exception {
    LOG.info("findWithAzureErrorInvalidCredentialsReturnsEmptyList");

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

    Assert.assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void findWithNonexistentResourceGroupReturnsEmptyList() throws Exception {
    LOG.info("findWithNonexistentResourceGroupReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Assert.assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void findWithNonexistentVirtualMachineReturnsEmptyList() throws Exception {
    LOG.info("findWithNonexistentVirtualMachineReturnsEmptyList");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Assert.assertEquals(0, provider.find(template, instanceIds).size());
  }

  @Test
  public void getInstanceStateWithInvalidCredentialsReturnsMapOfUnknowns() throws Exception {
    LOG.info("getInstanceStateWithInvalidCredentialsReturnsMapOfUnknowns");

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
    Assert.assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      Assert.assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void getInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns() throws Exception {
    LOG.info("getInstanceStateWithNonexistentResourceGroupReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      Assert.assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
    }
  }

  @Test
  public void getInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns() throws Exception {
    LOG.info("getInstanceStateWithNonexistentVirtualMachineReturnsMapOfUnknowns");

    Collection<String> instanceIds = new ArrayList<String>();
    instanceIds.add(UUID.randomUUID().toString());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        TestHelper.buildValidDirectorLiveTestConfig(), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    Map<String, InstanceState> vms = provider.getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), vms.size());
    for (Map.Entry<String, InstanceState> entry : vms.entrySet()) {
      Assert.assertEquals(InstanceStatus.UNKNOWN, entry.getValue().getInstanceStatus());
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
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
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
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), foundInstances.size());
    AzureComputeInstance instance = foundInstances.iterator().next(); // there's only one
    String publicIpId = instance.getInstanceDetails().getPrimaryPublicIPAddressId();

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
    Assert.assertEquals(0, provider.find(template, instanceIds).size());

    // 5. verify the public IP still exists
    Assert.assertNotNull(azure.publicIPAddresses().getById(publicIpId));

    // 6. cleanup: delete the public IP
    azure.publicIPAddresses().deleteById(publicIpId);

    // 7. cleanup: verify the public IP was deleted
    Assert.assertNull(azure.publicIPAddresses().getById(publicIpId));
  }

  @Ignore
  @Test
  public void allocateWithTheSameInstanceIdMultipleTimesExpectIdempotency() throws Exception {
    LOG.info("allocateWithTheSameInstanceIdMultipleTimesExpectIdempotency");

    // 0. set up
    LOG.info("0. set up");
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
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
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), foundInstances.size());

    // 3. verify that the instance is in the RUNNING state
    LOG.info("3. getInstanceState");
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      Assert.assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
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
    Assert.assertTrue(hitUnrecoverableProviderException);

    // 6. verify that the instance can still be found
    LOG.info("6. find");
    foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), foundInstances.size());

    // 7. verify that the instance is still in the RUNNING state
    LOG.info("7. getInstanceState");
    instanceStates = provider.getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), instanceStates.size());
    for (String instance : instanceIds) {
      Assert.assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // 8. delete the vm
    LOG.info("8. delete");
    provider.delete(template, instanceIds);

    // 9. verify that all the instances were correctly deleted
    LOG.info("9. find");
    Assert.assertEquals(0, provider.find(template, instanceIds).size());

    // 10. verify that all instances are in the UNKNOWN state
    LOG.info("10. getInstanceState");
    Map<String, InstanceState> instanceStatesDeleted = provider
        .getInstanceState(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), instanceStatesDeleted.size());
    for (String instance : instanceIds) {
      Assert.assertEquals(InstanceStatus.UNKNOWN, instanceStatesDeleted.get(instance)
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
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
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
    Collection<AzureComputeInstance> foundInstances = provider.find(template, singleInstanceIds);
    Assert.assertEquals(foundInstances.size(), 0);
  }

  /**
   * This tests that using a VM image that does not have a purchase plan attached works by deploying
   * the official RHEL 6.7 OS image (the RHEL images do not have purchase plans).
   *
   * @throws Exception
   */
  @Test
  public void allocateWithImageThatHasNoPurchasePlanExpectSuccess() throws Exception {
    LOG.info("allocateWithImageThatHasNoPurchasePlanExpectSuccess");

    // 0. set up
    LOG.info("0. set up");
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        TestHelper.TEST_RHEL_IMAGE_NAME);

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
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
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(instanceIds.size(), foundInstances.size());

    // 3. delete
    provider.delete(template, instanceIds);
  }

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
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null);
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
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
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instanceIds);
    Assert.assertEquals(1, foundInstances.size());

    // 3. delete VM
    LOG.info("3. delete VM");
    provider.delete(template, instanceIds);

    // 4. delete custom image
    LOG.info("4. delete custom image");
    CustomVmImageTestHelper.deleteCustomManagedVmImage(imageUri);
  }

  @Test
  public void createVmWithImplicitMsiAndAddToExistingAadGroup()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithImplicitMsiAndAddToExistingAadGroup");
    implicitMsiTestHelper(true,true);
  }

  @Test
  public void createVmWithImplicitMsiAndNotAddToExistingAadGroup()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithImplicitMsiAndNotAddToExistingAadGroup");
    implicitMsiTestHelper(true,false);
  }

  @Test
  public void createVmWithoutImplicitMsi()
      throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());
    LOG.info("createVmWithoutImplicitMsi");
    implicitMsiTestHelper(false,false);
  }

  /**
   * Helper function to run implicit MSI tests
   *
   * @param useImplicitMsi true for creating VM with implicit MSI
   * @param assignGroup true for adding implicit MSI to existing AAD group
   * @throws Exception
   */
  private void implicitMsiTestHelper(boolean useImplicitMsi, boolean assignGroup) throws Exception {
    // 0. set up
    LOG.info("0. set up");
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null);
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), useImplicitMsi ? "Yes" : "No");

    if (assignGroup) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
              .unwrap().getConfigKey(),
          TestHelper.TEST_AAD_GROUP_NAME);
    }

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(map), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the instanceId to use for this test
    String instanceId = UUID.randomUUID().toString();
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(instanceId);

    // 1. allocate the VMs, allow success even if none come up
    LOG.info("1. allocate");
    provider.allocate(template, instanceIds, 1);

    // 2. verify instances can be found
    LOG.info("2. find");
    ArrayList<AzureComputeInstance> foundInstances =
        (ArrayList<AzureComputeInstance>) provider.find(template, instanceIds);
    Assert.assertEquals(foundInstances.size(), 1);

    // 3. verify implicit MSI is created and present in group
    LOG.info("3. verify implicit MSI");
    GraphRbacManager graphRbacManager = TestHelper.getAzureCredentials().getGraphRbacManager();
    AzureComputeInstance vmInstance = foundInstances.get(0);
    String msiObjectId = vmInstance.getInstanceDetails().managedServiceIdentityPrincipalId();
    if (useImplicitMsi) {
      ServicePrincipal msiObject = graphRbacManager.servicePrincipals().getById(msiObjectId);
      Assert.assertNotNull(msiObject);
    } else {
      Assert.assertNull(msiObjectId);
    }

    if (assignGroup) {
      Set<ActiveDirectoryObject> members =
          graphRbacManager.groups().getByName(TestHelper.TEST_AAD_GROUP_NAME).listMembers();
      boolean found = false;
      for (ActiveDirectoryObject object : members) {
        if (object.id().equals(msiObjectId)) {
          LOG.info("Found implicit MSI: name {}, id {}.", object.name(), object.id());
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }

    // 4. delete vm
    LOG.info("4. delete VM");
    provider.delete(template, instanceIds);

    // 5. confirm vm deletion
    LOG.info("5. confirm vm deletion");
    Assert.assertEquals(0, provider.find(template, instanceIds).size());

    // wait a little for AAD to update
    Thread.sleep(10000);

    // 6. verify implicit MSI is removed (from AAD and from group)
    if (assignGroup) {
      LOG.info("6. verify implicit MSI deletion");
      Set<ActiveDirectoryObject> members =
          graphRbacManager.groups().getByName(TestHelper.TEST_AAD_GROUP_NAME).listMembers();
      boolean found = false;
      for (ActiveDirectoryObject object : members) {
        if (object.id().equals(msiObjectId)) {
          found = true;
          break;
        }
      }
      Assert.assertFalse(found);
    }
    // FIXME getById() throws an exception if service principal does not exist.
    // This is is an Azure Java SDK issue: https://github.com/Azure/azure-sdk-for-java/issues/1845
    try {
      graphRbacManager.servicePrincipals().getById(msiObjectId);
    } catch (Exception e) {
      LOG.error("Expected exception", e);
    }
  }
}
