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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import com.cloudera.director.azure.TestConfigHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceHelper;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.azure.utils.VmCreationParameters;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementService;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.Subnet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.VirtualNetwork;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.ResourceManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.ResourceManagementService;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.models.ResourceGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.models.AccountType;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.Configuration;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Live tests for exercising the Azure plugin.
 * <p>
 * Testing these AzureComputeProviderHelper.class methods:
 * - submitVmCreationTask()
 * - submitDeleteVmTask()
 */
public class AzureComputeProviderHelperLiveTest {
  private static final Logger LOG =
    LoggerFactory.getLogger(AzureComputeProviderHelperLiveTest.class);

  private TestConfigHelper cfgHelper;
  private AzureCredentials cred;
  private Configuration config;
  private ComputeManagementClient computeManagementClient;
  private AzureComputeProviderHelper helper;
  private TaskRunner taskRunner;
  private String vmNamePrefix;
  private String instanceId;
  private String vmName;
  private String fqdnSuffix;
  private String vmSize;
  private VirtualNetwork vnet;
  private Subnet subnet;
  private AvailabilitySet as;
  private NetworkSecurityGroup nsg;
  private ResourceContext context;
  private String resourceGroup;
  private AccountType storageAccountType;
  private int dataDiskCount;
  private int dataDiskSizeGiB;
  static final int RESOURCE_PREFIX_LENGTH = 8;
  static final String RESOURCE_NAME_TEMPLATE = "%s%s%s";
  private int azureOperationPollingTimeout;


  @BeforeClass
  public static void checkLiveTestFlag() {
    assumeTrue(TestConfigHelper.runLiveTests());
  }

  @Before
  public void setup() throws Exception {
    TestConfigHelper.seedAzurePluginConfigWithDefaults();

    cfgHelper = new TestConfigHelper();
    cred = cfgHelper.getAzureCredentials();
    config = cred.createConfiguration();
    computeManagementClient = ComputeManagementService.create(config);
    helper = new AzureComputeProviderHelper(config);
    taskRunner = TaskRunner.build();

    vmNamePrefix = "test";
    instanceId = UUID.randomUUID().toString();
    vmName = vmNamePrefix + "-" + instanceId;
    fqdnSuffix = "cdh-cluster.internal";
    vmSize = "Standard_DS2";
    vnet = helper.getVirtualNetworkByName(
      cfgHelper.getTestResourceGroup(),
      TestConfigHelper.DEFAULT_TEST_VIRTUAL_NETWORK);
    as = helper.getAvailabilitySetByName(
      cfgHelper.getTestResourceGroup(),
      TestConfigHelper.DEFAULT_TEST_AVAILABILITY_SET);
    nsg = helper.getNetworkSecurityGroupByName(
      cfgHelper.getTestResourceGroup(),
      TestConfigHelper.DEFAULT_TEST_NETWORK_SECURITY_GROUP);
    subnet = helper.getSubnetByName(
      cfgHelper.getTestResourceGroup(),
      TestConfigHelper.DEFAULT_TEST_VIRTUAL_NETWORK,
      TestConfigHelper.DEFAULT_TEST_SUBNET);
    storageAccountType = AccountType.valueOf(TestConfigHelper.DEFAULT_TEST_STORAGE_ACCOUNT_TYPE);
    dataDiskCount = 2;
    dataDiskSizeGiB = TestConfigHelper.DEFAULT_TEST_DATA_DISK_SIZE_GB;
    azureOperationPollingTimeout = TestConfigHelper.DEFAULT_AZURE_OPERATION_POLLING_TIMEOUT;
    context = buildContext(cred, vmName, vnet);
    resourceGroup = context.getResourceGroupName();
  }

  @After
  public void reset() throws Exception {
    cfgHelper = null;
    cred = null;
    config = null;
    computeManagementClient = null;
    helper = null;

    vmNamePrefix = null;
    instanceId = null;
    vmName = null;
    fqdnSuffix = null;
    vmSize = null;
    vnet = null;
    as = null;
    nsg = null;

    context = null;
  }

  /**
   * Tests the entire cycle of creating and destroying a VM on Azure.
   * <p>
   * To run these tests, you must create the following resources in Azure (East US region) first:
   * - ResourceGroup named: pluginUnitTestResourceGroup
   * - Virtual Network (under the resource group above) named: TestVnet
   * - Network Security Group (under the resource group above) named: TestNsg
   * - Availability Set (under the resource group above) named: TestAs
   * <p>
   * These tests can be run by:
   * mvn -Dtest.azure.live=true -DsubscriptionId=<sub_id> -DtenantId=<ten_id> -DclientId=<client_id>
   * -DclientSecret=<client_secret> -Dtest=AzureComputeProviderHelperLiveTest test
   *
   * @throws Exception if an exception occurs
   */
  @Test
  public void fullCycleTest() throws Exception {
    LOG.info("Start create vm " + vmName);
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();

    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(resourceGroup, vmName)
      .getInstanceStatus());

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    taskRunner.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  /**
   * Tests the entire cycle of creating and destroying a VM using standard storage on Azure.
   *
   * @throws Exception
   */
  @Test
  public void submitVmCreationTask_standardStorage_succeeds() throws Exception {
    LOG.info("Start create vm " + vmName);
    storageAccountType = AccountType.StandardLRS;
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();

    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(resourceGroup, vmName)
      .getInstanceStatus());

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    taskRunner.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  /**
   * This test verifies idempotency from Azure by creating a VM and then creating the same VM again
   * (same instanceId, same context). Under the hood submitVmCreationTask() will call
   * VirtualMachineOperations#beginCreatingOrUpdating(). The expected outcome from the second
   * beginCreatingOrUpdating() call is a VM update with the same context and "successful" return.
   *
   * @throws Exception
   */
  @Test
  public void submitVmCreationTask_doubleCreateVM_isIdempotentAndDoesNotError() throws Exception {
    LOG.info("Start (first of two) create VM {}.", vmName);
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    // verify that the vm has been successfully created
    assertEquals(1, taskRunner.pollPendingTask(vmr, 1800, 10));

    LOG.info("VM {} is created.", vmName);
    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(resourceGroup, vmName)
      .getInstanceStatus());

    LOG.info("Start (second of two) create VM {}.", vmName);

    // idempotent test
    Future<TaskResult> vmrTwo = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    // verify that a second submitVmCreationTask() with the same parameters doesn't error out
    taskRunner.pollPendingTask(vmrTwo, 1800, 10);
    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(resourceGroup, vmName)
      .getInstanceStatus());
    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete vm " + vmName);
    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(cfgHelper.getTestResourceGroup(), vmName)
      .getVirtualMachine();
    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    taskRunner.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      "VM " + vmName + " has not been deleted - check Azure console for orphaned resources",
      hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  @Test
  public void submitDeleteVmTask_doubleDeleteVM_isIdempotentAndDoesNotError() throws Exception {
    LOG.info("Start create vm " + vmName);
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();

    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(resourceGroup, vmName)
      .getInstanceStatus());

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete vm " + vmName);

    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    // the vm should be deleted
    assertEquals(1, taskRunner.pollPendingTask(deleteTask, 1800, 10));

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);

    Future<TaskResult> deleteAgain = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    // nothing should be deleted as it's already been deleted once
    assertEquals(0, taskRunner.pollPendingTask(deleteAgain, 1800, 10));

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    hitException = false;
    try {
      computeManagementClient
        .getVirtualMachinesOperations()
        .getWithInstanceView(cfgHelper.getTestResourceGroup(), vmName)
        .getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);
  }

  /**
   * Tests the entire cycle of creating and destroying a VM on Azure with the official RHEL 6.7
   * image. This test verifies using VM images that does not have purchase plan attached.
   *
   * @throws Exception if an exception occurs
   */
  @Test
  public void imageWithNoPurchasePlanTest() throws Exception {
    LOG.info("Start create vm {} with image {}", vmName, cfgHelper.getOfficialRhel67ImageInfo());
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getOfficialRhel67ImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    taskRunner.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        cfgHelper.getTestResourceGroup(), vmName).getVirtualMachine();
    } catch (ServiceException e) {
      LOG.debug("ServiceException:", e);
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  /**
   * Tests the AzureComputeInstanceHelper get methods:
   *   - getImageReference()
   *   - getInstanceID()
   *   - getInstanceType()
   *   - getPrivateIpAddress()
   *   - getPrivateFqdn()
   *   - getPublicIpAddress()
   *   - getPublicFqdn()
   *
   * @throws Exception
   */
  @Test
  public void AzureComputeInstanceDisplayPropertyToken_standardSetup_allTokensCorrect()
    throws Exception {
    LOG.info("Start create vm " + vmName);
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(cfgHelper.getTestResourceGroup(), vmName)
      .getVirtualMachine();

    // VM is up, test the AzureComputeInstanceHelper get methods
    AzureComputeInstanceHelper azureComputeInstanceHelper =
      helper.createAzureComputeInstanceHelper(vm, cred, context.getResourceGroupName());

    // getImageReference()
    assertNotNull(azureComputeInstanceHelper.getImageReference());
    assertEquals("CLOUDERA-CENTOS-6", azureComputeInstanceHelper.getImageReference());

    // getInstanceID()
    assertNotNull(azureComputeInstanceHelper.getInstanceID());

    // getInstanceType()
    assertNotNull(azureComputeInstanceHelper.getInstanceType());
    assertEquals("Standard_DS2", azureComputeInstanceHelper.getInstanceType());

    // getPrivateIpAddress()
    assertNotNull(azureComputeInstanceHelper.getPrivateIpAddress());

    // getPrivateFqdn()
    assertNotNull(azureComputeInstanceHelper.getPrivateFqdn());
    assertEquals(
      AzureComputeProviderHelper.getShortVMName(vmNamePrefix, instanceId) + "." + fqdnSuffix,
      azureComputeInstanceHelper.getPrivateFqdn());

    // getPublicIpAddress()
    assertNotNull(azureComputeInstanceHelper.getPublicIpAddress());

    // getPublicFqdn()
    assertNotNull(azureComputeInstanceHelper.getPublicFqdn());
    assertEquals(
      AzureComputeProviderHelper.getShortVMName(vmNamePrefix, instanceId) + "." +
        TestConfigHelper.DEFAULT_TEST_REGION + "." +
        TestConfigHelper.DEFAULT_TEST_PUBLIC_URL_POSTFIX,
      azureComputeInstanceHelper.getPublicFqdn());

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = taskRunner.submitDeleteVmTask(helper,
        context.getResourceGroupName(), vm,true, azureOperationPollingTimeout);

    taskRunner.pollPendingTask(deleteTask, 1800, 10);
  }

  /**
   * This test verifies VM can be created even the resource group name contains "_"
   *
   * @throws Exception if an exception occurs
   */
  @Test
  public void createVmInResourceGroupNameWithUnderscoreTest() throws Exception {
    ResourceGroup rg = new ResourceGroup(TestConfigHelper.DEFAULT_TEST_REGION);
    AvailabilitySet as = new AvailabilitySet(TestConfigHelper.DEFAULT_TEST_REGION);
    String asName = "rgas";
    as.setName(asName);
    String rgName = "rg_with_underscore";
    ResourceManagementClient resourceManagementClient = ResourceManagementService.create(config);
    resourceManagementClient.getResourceGroupsOperations().createOrUpdate(rgName, rg);
    computeManagementClient.getAvailabilitySetsOperations().createOrUpdate(rgName, as);

    context = new ResourceContext(
      TestConfigHelper.DEFAULT_TEST_REGION,
      rgName,
      cred.getSubscriptionId(),
      true);
    setupContext(context, vmName, vnet);
    context.setAvailabilitySetName(asName);
    as.setId(helper.createAndSetAvailabilitySetId(context));

    LOG.info("Start create vm " + vmName);
    VmCreationParameters parameters = new VmCreationParameters(vnet, subnet, nsg, as,
      vmSize, vmNamePrefix, instanceId, fqdnSuffix, TestConfigHelper.DEFAULT_TEST_SSH_USERNAME,
      TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, storageAccountType, dataDiskCount,
      dataDiskSizeGiB, cfgHelper.getDefaultImageInfo());
    Future<TaskResult> vmr = taskRunner.submitVmCreationTask(helper, context, parameters,
        azureOperationPollingTimeout);

    taskRunner.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    assertEquals(InstanceStatus.RUNNING, helper.getVirtualMachineStatus(rgName, vmName)
      .getInstanceStatus());

    helper.deleteResourceGroup(context);
  }


  private ResourceContext buildContext(AzureCredentials cred, String vmName, VirtualNetwork vnet) {
    ResourceContext context = new ResourceContext(
      TestConfigHelper.DEFAULT_TEST_REGION,
      cfgHelper.getTestResourceGroup(),
      cred.getSubscriptionId(),
      true);

    setupContext(context, vmName, vnet);
    return context;
  }

  private void setupContext(ResourceContext context, String vmName, VirtualNetwork vnet) {
    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("TestTag", "TestTagValue");
    context.setTags(tags);
    context.setVirtualNetworkName(vmName);
    context.setVirtualNetwork(vnet);
    context.setAvailabilitySetName(TestConfigHelper.DEFAULT_TEST_AVAILABILITY_SET);
    String resourcePrefix = instanceId.substring(0,8);
    String randomPadding = context.randomString(RESOURCE_PREFIX_LENGTH);

    context.setStorageAccountName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
      randomPadding, "sa"));
    context.setNetworkInterfaceName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
      randomPadding,  "nic"));
    context.setPublicIpName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
      randomPadding,  "publicip"));
    context.setContainerName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
      randomPadding,  "container"));
    context.setIpConfigName(String.format(RESOURCE_NAME_TEMPLATE, resourcePrefix,
      randomPadding,  "ipconfig"));
  }
}
