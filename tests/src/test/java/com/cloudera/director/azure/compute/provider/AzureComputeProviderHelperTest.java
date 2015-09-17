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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.AzureVirtualMachineState;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.DeleteOperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.InstanceViewStatus;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterfaceGetResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.ResourceId;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.UpdateOperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.VirtualNetwork;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.ResourceManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.models.ResourceGroupExtended;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.StorageManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.core.OperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Mock tests for AzureComputeProviderHelper class.
 */

public class AzureComputeProviderHelperTest {
  AzureComputeProviderHelper helper;
  NetworkResourceProviderClient networkResourceProviderClient;
  ComputeManagementClient computeManagementClient;
  StorageManagementClient storageManagementClient;
  ResourceManagementClient resourceManagementClient;
  NetworkInterfaceGetResponse networkInterfaceGetResponse;
  NetworkInterfaceIpConfiguration networkInterfaceIpConfiguration;
  ResourceId resourceId;
  OperationResponse operationResponse;
  NetworkInterface networkInterface;
  ExecutorService service;
  VirtualMachine vm;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private String niName = "cloudera_ni"; // ni = network interface
  private String niURI = "https://azure.com/providers/Microsoft.Network/networkInterfaces/" +
    niName;
  private String asName = "cloudera_as"; // as = availability set
  private String asURI = "https://azure.com/providers/Microsoft.Compute/availabilitySets/" + asName;
  private String saName = "cloudera_sa"; // sa == storage account
  private String saURI = "https://" + saName + ".blob.core.windows.net/";
  private String publicIPName = "default.public.ip.com";
  private String rgName = "default_rg";
  private ResourceGroupExtended rg;
  private ResourceGroupExtended vnrg;
  private String vnName = "default_vn";
  private VirtualNetwork vn;
  private ResourceGroupExtended nsgrg;
  private String nsgName = "default_nsg";
  private NetworkSecurityGroup nsg;
  private AvailabilitySet as;
  private String vmSize;
  private String vmName;
  private String fqdnSuffix;
  private String adminName;
  private String sshKey;
  private int dataDiskCount;
  private ResourceContext context;
  private AzureCredentials credentials;

  @Before
  public void setup() throws NoSuchMethodException, IllegalAccessException,
    InvocationTargetException, InstantiationException, NoSuchFieldException {
    computeManagementClient = mock(ComputeManagementClient.class, RETURNS_DEEP_STUBS);
    networkResourceProviderClient = mock(NetworkResourceProviderClient.class, RETURNS_DEEP_STUBS);
    storageManagementClient = mock(StorageManagementClient.class, RETURNS_DEEP_STUBS);
    resourceManagementClient = mock(ResourceManagementClient.class, RETURNS_DEEP_STUBS);
    networkInterfaceGetResponse = mock(NetworkInterfaceGetResponse.class, RETURNS_DEEP_STUBS);
    networkInterface = mock(NetworkInterface.class, RETURNS_DEEP_STUBS);
    networkInterfaceIpConfiguration = mock(NetworkInterfaceIpConfiguration.class,
      RETURNS_DEEP_STUBS);
    operationResponse = mock(OperationResponse.class, RETURNS_DEEP_STUBS);
    resourceId = mock(ResourceId.class, RETURNS_DEEP_STUBS);
    service = mock(ExecutorService.class, RETURNS_DEEP_STUBS);
    helper = spy(createHelperUsingReflection());
    vm = mock(VirtualMachine.class, RETURNS_DEEP_STUBS);
    rg = mock(ResourceGroupExtended.class);
    vnrg = mock(ResourceGroupExtended.class);
    vn = mock(VirtualNetwork.class);
    nsgrg = mock(ResourceGroupExtended.class);
    nsg = mock(NetworkSecurityGroup.class);
    as = mock(AvailabilitySet.class);
    vmSize = "STANDARD_DS14";
    vmName = "vmForTesting";
    fqdnSuffix = "cdh-cluster.internal";
    adminName = "cloudera";
    sshKey = "";
    dataDiskCount = 5;
    context = mock(ResourceContext.class);
    credentials = mock(AzureCredentials.class);
  }


  //
  // beginDeleteVirtualMachine() Tests
  //

  @Test
  public void beginDeleteVirtualMachine_validInput_returnsDeleteOperationResponse()
    throws IOException, ServiceException {
    when(computeManagementClient.getVirtualMachinesOperations().beginDeleting(anyString(),
      anyString()))
      .thenReturn(mock(DeleteOperationResponse.class));
    when(vm.getName()).thenReturn("vm");

    helper.beginDeleteVirtualMachine("rg", vm);

    verify(computeManagementClient.getVirtualMachinesOperations(), times(1))
      .beginDeleting(anyString(), anyString());
  }


  //
  // getShortVMName() Tests
  //

  @Test
  public void getShortVMName_anyInput_returnsShortened() throws Exception {
    String vmNamePrefix = "master";
    String instanceId = "7aa7cb8f-e89f-4588-8e71-f6e5a9f5d5ce";

    String shortVMName = AzureComputeProviderHelper.getShortVMName(vmNamePrefix, instanceId);

    assertEquals("master-7aa7cb8f", shortVMName);
  }


  //
  // getPublicFqdn() Tests
  //

  @Test
  public void getPublicFqdn_anyInput_returnsConcatenation() throws Exception {
    String vmShortName = "default";
    String regionName = "uswest";

    Class clazz = AzureComputeProviderHelper.class;
    Method method = clazz.getDeclaredMethod("getPublicFqdn", String.class, String.class);
    method.setAccessible(true);
    Field field = clazz.getDeclaredField("PUBLIC_URL_POSTFIX");
    field.setAccessible(true);

    String publicFqdn = (String) method.invoke(helper, vmShortName, regionName);
    String urlPostfix = (String) field.get(helper);

    assertEquals(vmShortName + "." + regionName + urlPostfix, publicFqdn);
  }


  //
  // getSshPath() Tests
  //

  @Test
  public void getSshPath_anyInput_returnsValidParsing() throws Exception {
    String adminUsername = "cloudera";
    String path = "/home/" + adminUsername + "/.ssh/authorized_keys";

    Class clazz = AzureComputeProviderHelper.class;
    Method method = clazz.getDeclaredMethod("getSshPath", String.class);
    method.setAccessible(true);

    String sshPath = (String) method.invoke(helper, adminUsername);

    assertEquals(path, sshPath);
  }


  //
  // beginDeleteStorageAccountOnVM() Tests
  //

  @Test
  public void beginDeleteStorageAccountOnVM_validInput_correctlyParses() throws Exception {
    String pipBase = "/subscriptions/e39b1984-855f-43b2-8c9d-4dffef728fe3/resourceGroups/pluginUn" +
      "itTestResourceGroup/providers/Microsoft.Network/publicIPAddresses/";
    String pip = "pluginUnitTestResourceGrouppublicipuqoks";

    when(vm.getNetworkProfile().getNetworkInterfaces().get(0).getReferenceUri())
      .thenReturn(niURI);
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .get(anyString(), anyString()))
      .thenReturn(networkInterfaceGetResponse);
    when(networkInterfaceGetResponse.getNetworkInterface())
      .thenReturn(networkInterface);
    when(networkInterface.getIpConfigurations().get(0))
      .thenReturn(networkInterfaceIpConfiguration);
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(anyString(), anyString()))
      .thenReturn(operationResponse);
    when(networkInterfaceIpConfiguration.getPublicIpAddress())
      .thenReturn(resourceId);
    when(networkInterfaceIpConfiguration.getPublicIpAddress().getId())
      .thenReturn(pipBase + pip);
    when(networkResourceProviderClient.getPublicIpAddressesOperations()
      .beginDeleting(anyString(), anyString()))
      .thenReturn(mock(UpdateOperationResponse.class));

    helper.beginDeleteNetworkResourcesOnVM(rgName, vm);

    // verifies that this method was called with the correct parsed out value
    verify(networkResourceProviderClient.getPublicIpAddressesOperations())
      .beginDeleting(rgName, pip);
  }


  //
  // getNicNameFromVm() Tests
  //

  @Test
  public void getNicNameFromVm_validInput_returnsValidMatch() throws Exception {
    when(vm.getNetworkProfile().getNetworkInterfaces().get(0).getReferenceUri())
      .thenReturn(niURI);

    assertEquals(helper.getNicNameFromVm(vm), niName);
  }

  @Test
  public void getNicNameFromVm_invalidInput_returnsNull() throws Exception {
    when(vm.getNetworkProfile().getNetworkInterfaces().get(0).getReferenceUri())
      .thenReturn("a-string-with-no-match");

    assertEquals(helper.getNicNameFromVm(vm), "");
  }


  //
  // getAvailabilitySetNameFromVm() Tests
  //

  @Test
  public void getAvailabilitySetNameFromVm_validInput_returnsValidMatch() throws Exception {
    when(vm.getAvailabilitySetReference().getReferenceUri())
      .thenReturn(asURI);

    assertEquals(helper.getAvailabilitySetNameFromVm(vm), asName);
  }

  @Test
  public void getAvailabilitySetNameFromVm_invalidInput_returnsNull() throws Exception {
    when(vm.getAvailabilitySetReference().getReferenceUri())
      .thenReturn("a-string-with-no-match");

    assertEquals(helper.getAvailabilitySetNameFromVm(vm), "");
  }


  //
  // pollPendingTasks() Tests
  //

  @Test
  public void pollPendingTasks_validShutdownWithFailedContexts_success() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);
    ResourceContext rc = mock(ResourceContext.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);
    when(tr.getContex())
      .thenReturn(rc);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();
    failedContexts.add(rc);

    int succeededCount = helper.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_validShutdownWithNoFailedContexts_success() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = null;

    int succeededCount = helper.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_validShutdownWithNonexistentContexts_error() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = helper.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_invalidShutdown_ExecutionException() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenThrow(new ExecutionException(null));

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = helper.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(0, succeededCount);
  }

  @Test
  public void pollPendingTasks_invalidShutdown_InterruptedException() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenThrow(new InterruptedException(null));

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = helper.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(0, succeededCount);
  }

  @Test
  public void pollPendingTasks_timeOut_terminateAllTasks() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);
    ResourceContext rc = mock(ResourceContext.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);
    when(tr.getContex())
      .thenReturn(rc);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();
    failedContexts.add(rc);

    int succeededCount = helper.pollPendingTasks(tasks, 0, 1, failedContexts);

    assertEquals(0, succeededCount);
  }


  //
  // shutdownTaskRunnerService() Tests
  //

  @Test
  public void shutdownTaskRunnerService_elapsedTimeoutShutdown_error() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(false);

    Class clazz = AzureComputeProviderHelper.class;
    Method method = clazz.getDeclaredMethod("shutdownTaskRunnerService");
    method.setAccessible(true);

    method.invoke(helper);

    verify(service).awaitTermination(anyLong(), any(TimeUnit.class));
  }

  @Test
  public void shutdownTaskRunnerService_interruptedShutdown_InterruptedExceptionCaught()
    throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenThrow(new InterruptedException());

    Class clazz = AzureComputeProviderHelper.class;
    Method method = clazz.getDeclaredMethod("shutdownTaskRunnerService");
    method.setAccessible(true);

    method.invoke(helper);

    verify(service).awaitTermination(anyLong(), any(TimeUnit.class));
  }


  //
  // getStorageAccountFromVM() Tests
  //

  @Test
  public void getStorageAccountFromVM_validInput_returnsValidMatch() throws Exception {
    when(vm.getStorageProfile().getOSDisk().getVirtualHardDisk().getUri())
      .thenReturn(saURI);

    assertEquals(helper.getStorageAccountFromVM(vm), saName);
  }

  @Test
  public void getStorageAccountFromVM_invalidInput_returnsNull() throws Exception {
    when(vm.getStorageProfile().getOSDisk().getVirtualHardDisk().getUri())
      .thenReturn("a-string-with-no-match");

    assertEquals(helper.getStorageAccountFromVM(vm), "");
  }


  //
  // getVirtualMachineStatus() Tests
  //

  @Test
  public void getVirtualMachineStatus_validInput_returnsRunningInstanceState() throws Exception {
    InstanceViewStatus status = new InstanceViewStatus();
    status.setCode(AzureVirtualMachineState.POWER_STATE_RUNNING);
    ArrayList<InstanceViewStatus> statuses = new ArrayList<InstanceViewStatus>();
    statuses.add(status);

    when(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()).getVirtualMachine())
      .thenReturn(vm);
    when(vm.getInstanceView().getStatuses())
      .thenReturn(statuses);

    InstanceState state = helper.getVirtualMachineStatus(rgName, vmName);

    assertEquals(InstanceStatus.RUNNING, state.getInstanceStatus());
    verify(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()), times(1))
      .getVirtualMachine();
  }

  @Test
  public void getVirtualMachineStatus_validInput_returnsStoppedInstanceState() throws Exception {
    InstanceViewStatus status = new InstanceViewStatus();
    status.setCode(AzureVirtualMachineState.POWER_STATE_DEALLOCATED);
    ArrayList<InstanceViewStatus> statuses = new ArrayList<InstanceViewStatus>();
    statuses.add(status);

    when(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()).getVirtualMachine())
      .thenReturn(vm);
    when(vm.getInstanceView().getStatuses())
      .thenReturn(statuses);

    InstanceState state = helper.getVirtualMachineStatus(rgName, vmName);

    assertEquals(InstanceStatus.STOPPED, state.getInstanceStatus());
    verify(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()), times(1)).getVirtualMachine();
  }

  @Test
  public void getVirtualMachineStatus_invalidInput_returnsUnknownInstanceState() throws Exception {
    InstanceViewStatus status = new InstanceViewStatus();
    status.setCode("UNKNOWN_STATE");
    ArrayList<InstanceViewStatus> statuses = new ArrayList<InstanceViewStatus>();
    statuses.add(status);

    when(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()).getVirtualMachine())
      .thenReturn(vm);
    when(vm.getInstanceView().getStatuses())
      .thenReturn(statuses);

    InstanceState state = helper.getVirtualMachineStatus(rgName, vmName);

    assertEquals(InstanceStatus.UNKNOWN, state.getInstanceStatus());
    verify(computeManagementClient.getVirtualMachinesOperations()
      .getWithInstanceView(anyString(), anyString()), times(1)).getVirtualMachine();
  }


  //
  // getVirtualNetworkByName() Tests
  //

  @Test
  public void getVirtualNetworkByName_validInput_returnsVirtualNetwork() throws Exception {
    when(networkResourceProviderClient.getVirtualNetworksOperations()
      .get(anyString(), anyString()).getVirtualNetwork())
      .thenReturn(mock(VirtualNetwork.class));

    helper.getVirtualNetworkByName(rgName, vnName);

    verify(networkResourceProviderClient.getVirtualNetworksOperations()
      .get(anyString(), anyString())).getVirtualNetwork();
  }


  //
  // getNetworkSecurityGroupByName() Tests
  //

  @Test
  public void getNetworkSecurityGroupByName_validInput_returnsNetworkSecurityGroup()
    throws Exception {
    when(networkResourceProviderClient.getNetworkSecurityGroupsOperations()
      .get(anyString(), anyString()).getNetworkSecurityGroup())
      .thenReturn(mock(NetworkSecurityGroup.class));

    helper.getNetworkSecurityGroupByName(rgName, nsgName);

    verify(networkResourceProviderClient.getNetworkSecurityGroupsOperations()
      .get(anyString(), anyString())).getNetworkSecurityGroup();
  }


  //
  // getAvailabilitySetByName() Tests
  //

  @Test
  public void getAvailabilitySetByName_validInput_returnsAvailabilitySet() throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().get(anyString(), anyString())
      .getAvailabilitySet())
      .thenReturn(mock(AvailabilitySet.class));

    helper.getAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations().get(rgName, asName))
      .getAvailabilitySet();
  }


  //
  // beginDeleteStorageAccountByName() Tests
  //

  @Test
  public void beginDeleteStorageAccountByName_validInput_returnsOperationResponse()
    throws Exception {
    OperationResponse or = mock(OperationResponse.class);
    when(storageManagementClient.getStorageAccountsOperations().delete(anyString(), anyString()))
      .thenReturn(or);

    helper.beginDeleteStorageAccountByName(rgName, saName);

    verify(storageManagementClient.getStorageAccountsOperations()).delete(rgName, saName);
  }


  //
  // beginDeleteAvailabilitySetByName() Tests
  //

  @Test
  public void beginDeleteAvailabilitySetByName_validInput_returnsOperationResponse()
    throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().delete(anyString(), anyString()))
      .thenReturn(mock(OperationResponse.class));

    helper.beginDeleteAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteAvailabilitySetByName_anyInput_throwsServiceException() throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().delete(anyString(), anyString()))
      .thenThrow(ServiceException.class);

    exception.expect(ServiceException.class);
    helper.beginDeleteAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteAvailabilitySetByName_anyInput_throwsExecutionException()
    throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().delete(anyString(), anyString()))
      .thenThrow(ExecutionException.class);

    exception.expect(ExecutionException.class);
    helper.beginDeleteAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteAvailabilitySetByName_anyInput_throwsInterruptedException()
    throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().delete(anyString(), anyString()))
      .thenThrow(InterruptedException.class);

    exception.expect(InterruptedException.class);
    helper.beginDeleteAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteAvailabilitySetByName_anyInput_throwsIOException() throws Exception {
    when(computeManagementClient.getAvailabilitySetsOperations().delete(anyString(), anyString()))
      .thenThrow(IOException.class);

    exception.expect(IOException.class);
    helper.beginDeleteAvailabilitySetByName(rgName, asName);

    verify(computeManagementClient.getAvailabilitySetsOperations())
      .delete(anyString(), anyString());
  }


  //
  // beginDeleteNetworkInterfaceByName() Tests
  //

  @Test
  public void beginDeleteNetworkInterfaceByName_validInput_returnsOperationResponse()
    throws Exception {
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(anyString(), anyString()))
      .thenReturn(mock(OperationResponse.class));

    helper.beginDeleteNetworkInterfaceByName(rgName, niName);

    verify(networkResourceProviderClient.getNetworkInterfacesOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteNetworkInterfaceByName_anyInput_throwsInterruptedException()
    throws Exception {
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(InterruptedException.class);

    exception.expect(InterruptedException.class);
    helper.beginDeleteNetworkInterfaceByName(rgName, niName);

    verify(networkResourceProviderClient.getNetworkInterfacesOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteNetworkInterfaceByName_anyInput_throwsExecutionException()
    throws Exception {
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(ExecutionException.class);

    exception.expect(ExecutionException.class);
    helper.beginDeleteNetworkInterfaceByName(rgName, niName);

    verify(networkResourceProviderClient.getNetworkInterfacesOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeleteNetworkInterfaceByName_anyInput_throwsIOException() throws Exception {
    when(networkResourceProviderClient.getNetworkInterfacesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(IOException.class);

    exception.expect(IOException.class);
    helper.beginDeleteNetworkInterfaceByName(rgName, niName);

    verify(networkResourceProviderClient.getNetworkInterfacesOperations())
      .delete(anyString(), anyString());
  }


  //
  // beginDeletePublicIpAddressByName() Tests
  //

  @Test
  public void beginDeletePublicIpAddressByName_validInput_returnsOperationResponse()
    throws Exception {
    when(networkResourceProviderClient.getPublicIpAddressesOperations()
      .delete(anyString(), anyString()))
      .thenReturn(mock(OperationResponse.class));

    helper.beginDeletePublicIpAddressByName(rgName, publicIPName);

    verify(networkResourceProviderClient.getPublicIpAddressesOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeletePublicIpAddressByName_invalidInput_InterruptedException()
    throws Exception {
    when(networkResourceProviderClient.getPublicIpAddressesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(new InterruptedException());

    exception.expect(InterruptedException.class);
    helper.beginDeletePublicIpAddressByName(rgName, publicIPName);

    verify(networkResourceProviderClient).getPublicIpAddressesOperations()
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeletePublicIpAddressByName_invalidInput_ExecutionException() throws Exception {
    when(networkResourceProviderClient.getPublicIpAddressesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(new ExecutionException(null));

    exception.expect(ExecutionException.class);
    helper.beginDeletePublicIpAddressByName(rgName, publicIPName);

    verify(networkResourceProviderClient.getPublicIpAddressesOperations())
      .delete(anyString(), anyString());
  }

  @Test
  public void beginDeletePublicIpAddressByName_invalidInput_IOException() throws Exception {
    when(networkResourceProviderClient.getPublicIpAddressesOperations()
      .delete(anyString(), anyString()))
      .thenThrow(new IOException());

    exception.expect(IOException.class);
    helper.beginDeletePublicIpAddressByName(rgName, publicIPName);

    verify(networkResourceProviderClient.getPublicIpAddressesOperations())
      .delete(anyString(), anyString());
  }

  private AzureComputeProviderHelper createHelperUsingReflection() throws IllegalAccessException,
    InvocationTargetException, InstantiationException, NoSuchMethodException, NoSuchFieldException {
    Class helperClazz = AzureComputeProviderHelper.class;
    Constructor<AzureComputeProviderHelper> constructor =
      (Constructor<AzureComputeProviderHelper>) helperClazz.getDeclaredConstructor();
    constructor.setAccessible(true);
    AzureComputeProviderHelper obj = constructor.newInstance();

    Field computeManagementClientField = helperClazz.getDeclaredField("computeManagementClient");
    computeManagementClientField.setAccessible(true);
    computeManagementClientField.set(obj, computeManagementClient);

    Field networkResourceProviderClientField = helperClazz.getDeclaredField(
      "networkResourceProviderClient");
    networkResourceProviderClientField.setAccessible(true);
    networkResourceProviderClientField.set(obj, networkResourceProviderClient);

    Field storageClientField = helperClazz.getDeclaredField("storageManagementClient");
    storageClientField.setAccessible(true);
    storageClientField.set(obj, storageManagementClient);

    Field resourceManagementClientField = helperClazz.getDeclaredField("resourceManagementClient");
    resourceManagementClientField.setAccessible(true);
    resourceManagementClientField.set(obj, resourceManagementClient);

    Field executorServiceField = helperClazz.getDeclaredField("service");
    executorServiceField.setAccessible(true);
    executorServiceField.set(obj, service);

    return obj;
  }
}
