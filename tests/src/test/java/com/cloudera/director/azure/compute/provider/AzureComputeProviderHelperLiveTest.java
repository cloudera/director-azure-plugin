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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import com.cloudera.director.azure.TestConfigHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementService;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.Subnet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.VirtualNetwork;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.Configuration;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
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

  TestConfigHelper cfgHelper;
  AzureCredentials cred;
  Configuration config;
  ComputeManagementClient computeManagementClient;
  AzureComputeProviderHelper helper;
  String vmNamePrefix;
  String instanceId;
  String vmName;
  String fqdnSuffix;
  String vmSize;
  VirtualNetwork vnet;
  Subnet subnet;
  AvailabilitySet as;
  NetworkSecurityGroup nsg;
  ResourceContext context;

  @BeforeClass
  public static void checkLiveTestFlag() {
    assumeTrue(TestConfigHelper.runLiveTests());
  }

  @Before
  public void setup() throws Exception {
    cfgHelper = new TestConfigHelper();
    cred = cfgHelper.getAzureCredentials();
    config = cred.createConfiguration();
    computeManagementClient = ComputeManagementService.create(config);
    helper = new AzureComputeProviderHelper(config);

    vmNamePrefix = "test";
    instanceId = UUID.randomUUID().toString();
    vmName = vmNamePrefix + "-" + instanceId;
    fqdnSuffix = "test.domain.com";
    vmSize = "Standard_DS2";
    vnet = helper.getVirtualNetworkByName(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP,
      TestConfigHelper.DEFAULT_TEST_VIRTUAL_NETWORK);
    as = helper.getAvailabilitySetByName(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP,
      TestConfigHelper.DEFAULT_TEST_AVAILABILITY_SET);
    nsg = helper.getNetworkSecurityGroupByName(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP,
      TestConfigHelper.DEFAULT_TEST_NETWORK_SECURITY_GROUP);
    subnet = helper.getSubnetByName(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP,
      TestConfigHelper.DEFAULT_TEST_VIRTUAL_NETWORK,
      TestConfigHelper.DEFAULT_TEST_SUBNET);

    context = buildContext(cred, vmName, vnet);
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
    Future<TaskResult> vmr = helper.submitVmCreationTask(
      context, vnet, subnet, nsg, as, vmSize, vmNamePrefix, instanceId, fqdnSuffix,
      TestConfigHelper.DEFAULT_TEST_SSH_USERNAME, TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, 2,
      cfgHelper.getDefaultImageInfo());

    helper.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    // FIXME this is kind of a find test, change to use ComputeProvider's API instead
    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();

    // FIXME add VM status test

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = helper.submitDeleteVmTask(context.getResourceGroupName(), vm);

    helper.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();
    } catch (ServiceException e) {
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
    Future<TaskResult> vmr = helper.submitVmCreationTask(
      context, vnet, subnet, nsg, as, vmSize, vmNamePrefix, instanceId, fqdnSuffix,
      TestConfigHelper.DEFAULT_TEST_SSH_USERNAME, TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, 2,
      cfgHelper.getDefaultImageInfo());

    // verify that the vm has been successfully created
    assertEquals(1, helper.pollPendingTask(vmr, 1800, 10));

    LOG.info("VM {} is created.", vmName);

    // FIXME this is kind of a find test, change to use ComputeProvider's API instead
    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName)
      .getVirtualMachine();

    LOG.info("Start (second of two) create VM {}.", vmName);

    // idempotent test
    Future<TaskResult> vmrTwo = helper.submitVmCreationTask(
      context, vnet, subnet, nsg, as, vmSize, vmNamePrefix, instanceId, fqdnSuffix,
      TestConfigHelper.DEFAULT_TEST_SSH_USERNAME, TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, 2,
      cfgHelper.getDefaultImageInfo());

    // verify that a second submitVmCreationTask() with the same parameters returns successfully
    assertEquals(1, helper.pollPendingTask(vmrTwo, 1800, 10));
    LOG.info("VM {} is created.", vmName);

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete vm " + vmName);

    Future<TaskResult> deleteTask = helper.submitDeleteVmTask(context.getResourceGroupName(), vm);

    helper.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();
    } catch (ServiceException e) {
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      "VM " + vmName + " has not been deleted - check Azure console for orphaned resources",
      hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  @Test
  public void submitDeleteVmTask_doubleDeleteVM_isIdempotentAndDoesnotError() throws Exception {
    LOG.info("Start create vm " + vmName);
    Future<TaskResult> vmr = helper.submitVmCreationTask(
      context, vnet, subnet, nsg, as, vmSize, vmNamePrefix, instanceId, fqdnSuffix,
      TestConfigHelper.DEFAULT_TEST_SSH_USERNAME, TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, 2,
      cfgHelper.getDefaultImageInfo());

    helper.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    // FIXME this is kind of a find test, change to use ComputeProvider's API instead
    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();

    // FIXME add VM status test

    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete vm " + vmName);

    Future<TaskResult> deleteTask = helper.submitDeleteVmTask(context.getResourceGroupName(), vm);

    // the vm should be deleted
    assertEquals(1, helper.pollPendingTask(deleteTask, 1800, 10));

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();
    } catch (ServiceException e) {
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);

    Future<TaskResult> deleteAgain = helper.submitDeleteVmTask(context.getResourceGroupName(), vm);

    // nothing should be deleted as it's already been deleted once
    assertEquals(0, helper.pollPendingTask(deleteAgain, 1800, 10));

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    hitException = false;
    try {
      computeManagementClient
        .getVirtualMachinesOperations()
        .getWithInstanceView(TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName)
        .getVirtualMachine();
    } catch (ServiceException e) {
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
    Future<TaskResult> vmr = helper.submitVmCreationTask(
      context, vnet, subnet, nsg, as, vmSize, vmNamePrefix, instanceId, fqdnSuffix,
      TestConfigHelper.DEFAULT_TEST_SSH_USERNAME, TestConfigHelper.DEFAULT_TEST_SSH_PUBLIC_KEY, 2,
      cfgHelper.getOfficialRhel67ImageInfo());

    helper.pollPendingTask(vmr, 1800, 10);
    LOG.info("VM {} is created.", vmName);

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();
    // wait a little before deleting the VM and its supporting resources.
    Thread.sleep(10 * 1000);

    LOG.info("Start delete VM {}.", vmName);

    Future<TaskResult> deleteTask = helper.submitDeleteVmTask(context.getResourceGroupName(), vm);

    helper.pollPendingTask(deleteTask, 1800, 10);

    // Verify VM has been deleted by trying to find it again, should hit Azure service exception
    boolean hitException = false;
    try {
      computeManagementClient.getVirtualMachinesOperations().getWithInstanceView(
        TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP, vmName).getVirtualMachine();
    } catch (ServiceException e) {
      assertTrue(e.getMessage().contains(vmName));
      hitException = true;
    }
    assertTrue(
      String.format("VM '%s' has not been deleted - check Azure console for orphaned resources",
        vmName), hitException);

    LOG.info("VM {} deleted.", vmName);
  }

  private ResourceContext buildContext(AzureCredentials cred, String vmName, VirtualNetwork vnet) {
    ResourceContext context = new ResourceContext(
      TestConfigHelper.DEFAULT_TEST_REGION,
      TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP,
      cred.getSubscriptionId(),
      true);

    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("TestTag", "TestTagValue");
    context.setTags(tags);
    context.setVirtualNetworkName(vmName);
    context.setVirtualNetwork(vnet);
    context.setAvailabilitySetName(TestConfigHelper.DEFAULT_TEST_AVAILABILITY_SET);

    return context;
  }
}
