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
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.util.SimpleInstanceState;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.ComputeLongRunningOperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.ComputeOperationStatus;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.DeleteOperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.PublicIpAddress;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.core.OperationResponse;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests to verify CleanUpTask.
 */
public class CleanUpTaskTest {
  AzureComputeProviderHelper helper = mock(AzureComputeProviderHelper.class);
  ResourceContext context = mock(ResourceContext.class);
  VirtualMachine vm = mock(VirtualMachine.class);
  String resourceGroup = "rg";
  String vmName = "VmToBeDeleted";
  String asName = "asName";
  String nicName = "nic";
  String saName = "sa";
  String pipName = "pip";
  ExecutorService service = Executors.newCachedThreadPool();
  PublicIpAddress pip = mock(PublicIpAddress.class);
  NetworkInterface nic = mock(NetworkInterface.class);
  DeleteOperationResponse dor = mock(DeleteOperationResponse.class);
  OperationResponse or = mock(OperationResponse.class);
  ComputeLongRunningOperationResponse clror = mock(ComputeLongRunningOperationResponse.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    when(vm.getName()).thenReturn(vmName);
    when(or.getStatusCode()).thenReturn(200);
  }

  @Test
  public void cleanUpProvisionFailureUsingContext() throws Exception {

    when(context.getVMInput()).thenReturn(vm);
    when(context.getNetworkInterfaceName()).thenReturn(nicName);
    when(context.getStorageAccountName()).thenReturn(saName);
    when(context.getPublicIpName()).thenReturn(pipName);
    when(context.getNetworkInterface()).thenReturn(nic);
    when(context.getPublicIpAddress()).thenReturn(pip);

    when(helper.getVirtualMachineStatus(resourceGroup, vmName)).thenThrow(
      new ServiceException("VM " + vmName + " does not exist in ResourceGroup " + resourceGroup));
    when(helper.beginDeleteStorageAccountByName(anyString(), anyString())).thenReturn(or);
    when(helper.beginDeletePublicIpAddressByName(anyString(), anyString())).thenReturn(or);
    when(helper.beginDeleteNetworkInterfaceByName(anyString(), anyString())).thenReturn(or);
    when(helper.getNicNameFromVm(vm)).thenReturn(nicName);
    when(helper.getStorageAccountFromVM(vm)).thenReturn(saName);
    when(helper.getAvailabilitySetNameFromVm(vm)).thenReturn(asName);
    when(helper.getResourceContextFromVm(anyString(), any(VirtualMachine.class))).thenReturn(context);

    CleanUpTask task = new CleanUpTask(resourceGroup, context, helper, true);
    Future<TaskResult> future = service.submit(task);
    TaskResult result = future.get(600, TimeUnit.SECONDS);
    verify(helper).beginDeleteStorageAccountByName(anyString(), anyString());
    verify(helper).beginDeletePublicIpAddressByName(anyString(), anyString());
    verify(helper).beginDeleteNetworkInterfaceByName(anyString(), anyString());
    assertTrue(result.isSuccessful());
  }

  @Test
  public void cleanUpProvisionedVirtualMachineResources() throws Exception {
    when(helper.getVirtualMachineStatus(resourceGroup, vmName)).thenReturn(new SimpleInstanceState(InstanceStatus
        .RUNNING));
    when(helper.beginDeleteVirtualMachine(resourceGroup, vm)).thenReturn(dor);
    when(helper.beginDeleteNetworkResourcesOnVM(resourceGroup, vm, true)).thenReturn(or);
    when(helper.beginDeleteStorageAccountOnVM(resourceGroup, vm)).thenReturn(or);
    when(helper.getLongRunningOperationStatus(anyString())).thenReturn(clror);
    when(clror.getStatus()).thenReturn(ComputeOperationStatus.InProgress);
    when(clror.getStatus()).thenReturn(ComputeOperationStatus.Succeeded);

    when(helper.getNicNameFromVm(any(VirtualMachine.class))).thenReturn(nicName);
    when(helper.getStorageAccountFromVM(any(VirtualMachine.class))).thenReturn(saName);
    when(helper.getAvailabilitySetNameFromVm(any(VirtualMachine.class))).thenReturn(asName);
    when(helper.getResourceContextFromVm(anyString(), any(VirtualMachine.class))).thenReturn(context);

    CleanUpTask task = new CleanUpTask(resourceGroup, vm, helper, true);
    Future<TaskResult> future = service.submit(task);
    TaskResult result = future.get(600, TimeUnit.SECONDS);
    verify(helper).beginDeleteStorageAccountOnVM(resourceGroup, vm);
    verify(helper).beginDeleteNetworkResourcesOnVM(resourceGroup, vm, true);
    assertTrue(result.isSuccessful());
  }
}
