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

import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.IMAGE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VMSIZE;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_OPENSSH_PUBLIC_KEY;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceHelper;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.azure.utils.AzureVirtualMachineState;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.model.util.SimpleInstanceState;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineOperations;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.InstanceViewStatus;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.PurchasePlan;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachineGetResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachineImage;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachineInstanceView;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.Subnet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.VirtualNetwork;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.models.ResourceGroupExtended;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.models.StorageAccount;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.Configuration;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

/**
 * Mock tests for AzureComputeProvider
 */
public class AzureComputeProviderTest {

  private AzureComputeProvider computeProvider;
  private ResourceContext context;
  private Configured configuration;
  private AzureCredentials credentials;
  private LocalizationContext localizationContext;
  private AzureComputeProviderHelper computeProviderHelper;
  private Config pluginConfig;
  private Config configurableImages;
  private AzureComputeInstanceTemplate template;
  private String vmNamePrefix = "director";
  private String instanceId = UUID.randomUUID().toString();
  private String vmName = vmNamePrefix + "-" + instanceId;
  private String vmSize = "vmsize";
  private String userName = "username";
  private String location = "location";
  private String resourceGroup = "resourcegroup";
  private String publicKey = "publickey";
  private int dataDiskCount = 5;
  private Future response = mock(Future.class);
  private VirtualNetwork vnet = mock(VirtualNetwork.class);
  private Subnet subnet = mock(Subnet.class);
  private NetworkSecurityGroup nsg = mock(NetworkSecurityGroup.class);
  private AvailabilitySet as = mock(AvailabilitySet.class);
  private String vnetName = "vnet";
  private String subnetName = "default";
  private String vnetRg = "vnetRg";
  private String nsgName = "nsg";
  private String availabilitySet = "asn";
  private String privateDomainName = "test.domain.com";
  private ComputeManagementClient client = mock(ComputeManagementClient.class);
  private VirtualMachineOperations vmop = mock(VirtualMachineOperations.class);
  private VirtualMachineGetResponse vmgr = mock(VirtualMachineGetResponse.class);
  private VirtualMachine vm = mock(VirtualMachine.class);
  private VirtualMachineInstanceView iv = mock(VirtualMachineInstanceView.class);
  private VirtualMachineImage vmimage = mock(VirtualMachineImage.class);
  private PurchasePlan purchasePlan = mock(PurchasePlan.class);
  private AzureVmImageInfo imageInfo =
    new AzureVmImageInfo("cloudera", "CLOUDERA-CENTOS-6", "cloudera-centos-6", "latest");
  private static final DefaultLocalizationContext DEFAULT_LOCALIZATION_CONTEXT =
    new DefaultLocalizationContext(Locale.getDefault(), "");
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    configuration = mock(Configured.class);
    credentials = mock(AzureCredentials.class);
    context = mock(ResourceContext.class);
    localizationContext = mock(LocalizationContext.class);
    when(localizationContext.getKeyPrefix()).thenReturn("ID");
    when(localizationContext.getLocale()).thenReturn(new Locale("en"));
    computeProviderHelper = mock(AzureComputeProviderHelper.class);
    when(credentials.getComputeProviderHelper()).thenReturn(computeProviderHelper);
    pluginConfig = AzurePluginConfigHelper.parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);
    configurableImages = AzurePluginConfigHelper.parseConfigFromClasspath(
      Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    computeProvider = new AzureComputeProvider(configuration, credentials, pluginConfig,
      configurableImages, localizationContext);

    when(credentials.getSubscriptionId()).thenReturn("subscription");
    when(credentials.createConfiguration()).thenReturn(mock(Configuration.class));
    when(credentials.createResourceContext(anyString(), anyString(), anyBoolean())).thenReturn(context);
    when(context.getStorageAccount()).thenReturn(mock(StorageAccount.class));
    when(context.getNetworkInterface()).thenReturn(mock(NetworkInterface.class));
    when(context.getAvailabilitySetId()).thenReturn("ASID");
    when(context.getLocation()).thenReturn("location").thenReturn("location");


    when(computeProviderHelper.getMarketplaceVMImage(anyString(), any(AzureVmImageInfo.class))).thenReturn(vmimage);
    when(computeProviderHelper.getPurchasePlan(vmimage)).thenReturn(purchasePlan);
    when(computeProviderHelper.getVirtualNetworkByName(anyString(), anyString())).thenReturn(vnet);
    when(computeProviderHelper.getSubnetByName(anyString(), anyString(), anyString())).thenReturn(subnet);
    when(computeProviderHelper.getNetworkSecurityGroupByName(anyString(), anyString())).thenReturn(nsg);
    when(computeProviderHelper.getAvailabilitySetByName(anyString(), anyString())).thenReturn(as);

    when(vmop.getWithInstanceView(resourceGroup, instanceId)).thenReturn(vmgr);
    when(vmgr.getVirtualMachine()).thenReturn(vm);
    when(client.getVirtualMachinesOperations()).thenReturn(vmop);
    when(vm.getInstanceView()).thenReturn(iv);

    // build a real instance template object
    HashMap<String, String> templateCfgMap = new HashMap<String, String>();
    templateCfgMap.put(IMAGE.unwrap().getConfigKey(), "cloudera-centos-6-latest");
    templateCfgMap.put(VMSIZE.unwrap().getConfigKey(), vmSize);
    templateCfgMap.put(COMPUTE_RESOURCE_GROUP.unwrap().getConfigKey(), resourceGroup);
    templateCfgMap.put(VIRTUAL_NETWORK_RESOURCE_GROUP.unwrap().getConfigKey(), vnetRg);
    templateCfgMap.put(VIRTUAL_NETWORK.unwrap().getConfigKey(), vnetName);
    templateCfgMap.put(SUBNET_NAME.unwrap().getConfigKey(), subnetName);
    templateCfgMap.put(HOST_FQDN_SUFFIX.unwrap().getConfigKey(), privateDomainName);
    templateCfgMap.put(NETWORK_SECURITY_GROUP_RESOURCE_GROUP.unwrap().getConfigKey(), "nsgRg");
    templateCfgMap.put(NETWORK_SECURITY_GROUP.unwrap().getConfigKey(), nsgName);
    templateCfgMap.put(PUBLIC_IP.unwrap().getConfigKey(), "Yes");
    templateCfgMap.put(AVAILABILITY_SET.unwrap().getConfigKey(), availabilitySet);
    templateCfgMap.put(DATA_DISK_COUNT.unwrap().getConfigKey(), ("" + dataDiskCount));
    templateCfgMap.put(SSH_USERNAME.unwrap().getConfigKey(), userName);
    templateCfgMap.put(SSH_OPENSSH_PUBLIC_KEY.unwrap().getConfigKey(), publicKey);

    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("owner", "testUser");

    SimpleConfiguration templateConfig = new SimpleConfiguration(templateCfgMap);
    template = new AzureComputeInstanceTemplate(
      "TestInstanceTemplate", templateConfig, tags, DEFAULT_LOCALIZATION_CONTEXT);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testFindVmResourceGroupDontExist() throws Exception {
    ArrayList list = new ArrayList();
    list.add(instanceId);
    assertEquals(0, computeProvider.find(template, list).size());
  }

  @Test
  public void testFindWrongVM() throws Exception {
    ArrayList groups = new ArrayList();
    ResourceGroupExtended resourceGroup = mock(ResourceGroupExtended.class);
    groups.add(resourceGroup);
    when(computeProviderHelper.getResourceGroups()).thenReturn(groups);
    ArrayList vms = new ArrayList();
    VirtualMachine vm = mock(VirtualMachine.class);
    vms.add(vm);
    when(computeProviderHelper.getVirtualMachines(anyString())).thenReturn(vms);
    when(vm.getName()).thenReturn("WILLNOTMATCH");

    ArrayList list = new ArrayList();
    list.add(instanceId);
    assertEquals(0, computeProvider.find(template, list).size());
  }

  @Test
  public void testFindMatchedVM() throws Exception {
    ArrayList vms = new ArrayList();
    VirtualMachine vm = mock(VirtualMachine.class);
    vms.add(vm);
    when(computeProviderHelper.getVirtualMachines(anyString())).thenReturn(vms);
    when(vm.getName()).thenReturn(vmName);
    when(computeProviderHelper.createAzureComputeInstanceHelper(vm, credentials, resourceGroup))
      .thenReturn(mock(AzureComputeInstanceHelper.class));

    ArrayList list = new ArrayList();
    list.add(instanceId);
    assertEquals(1, computeProvider.find(template, list).size());
  }

  @Test
  public void testDeleteSingleVM() throws Exception {
    ArrayList list = new ArrayList();
    list.add(instanceId);

    ArrayList vms = new ArrayList();
    VirtualMachine vm = mock(VirtualMachine.class);
    vms.add(vm);
    when(computeProviderHelper.getVirtualMachines(anyString())).thenReturn(vms);
    when(vm.getName()).thenReturn(vmName);
    when(computeProviderHelper.createAzureComputeInstanceHelper(vm, credentials, resourceGroup))
      .thenReturn(mock(AzureComputeInstanceHelper.class));

    Future<TaskResult> task = mock(Future.class);
    when(task.isDone()).thenReturn(true);
    when(task.get()).thenReturn(new TaskResult(true, null));
    when(computeProviderHelper.submitDeleteVmTask(resourceGroup, vm)).thenReturn(task);
    when(computeProviderHelper.pollPendingTask(any(Future.class), anyInt(), anyInt()))
      .thenCallRealMethod();
    when(computeProviderHelper.pollPendingTasks(anySet(), anyInt(), anyInt(), anySet()))
      .thenCallRealMethod();
    computeProvider.delete(template, list);

    assertEquals(1, computeProvider.getLastSuccessfulDeletionCount());
  }

  @Test
  public void testAllocateSingleVM() throws Exception {
    when(computeProviderHelper.submitVmCreationTask(context, vnet, subnet, nsg, as, vmSize,
      vmNamePrefix, instanceId, privateDomainName, userName, publicKey, dataDiskCount, imageInfo))
      .thenReturn(response);
    when(computeProviderHelper.pollPendingTasks(anySet(), anyInt(), anyInt(), anySet()))
      .thenReturn(1);

    ArrayList list = new ArrayList();
    list.add(instanceId);

    when(configuration.getConfigurationValue(any(ConfigurationPropertyToken.class),
      any(LocalizationContext.class)))
      .thenReturn(location);

    computeProvider.allocate(template, list, 1);

    verify(computeProviderHelper).submitVmCreationTask(context, vnet, subnet, nsg, as, vmSize,
      vmNamePrefix, instanceId, privateDomainName, userName, publicKey, dataDiskCount, imageInfo);
    assertEquals(1, computeProvider.getLastSuccessfulAllocationCount());
  }

  @Test
  public void testAllocateWithInterruptedException() throws Exception {
    thrown.expect(InterruptedException.class);
    when(computeProviderHelper.submitVmCreationTask(context, vnet, subnet, nsg, as, vmSize,
      vmNamePrefix, instanceId, privateDomainName, userName, publicKey, dataDiskCount, imageInfo))
      .thenReturn(response);
    when(computeProviderHelper.pollPendingTasks(anySet(), anyInt(), anyInt(), anySet()))
      .thenThrow(InterruptedException.class);

    ArrayList list = new ArrayList();
    list.add(instanceId);

    computeProvider.allocate(template, list, 1);
    verify(computeProviderHelper).deleteResources(anyString(), anySet());
  }

  @Test
  public void testAllocateMoreThanRequiredLessThanRequested() throws Exception {
    int expectedSuccess = 3;
    int expectedFailure = 1;
    ArrayList<String> instances = new ArrayList<String>();
    ArrayList<ResourceContext> contexts = new ArrayList<ResourceContext>();

    for (int i = 0; i < 4; i++) {
      String tempInstanceId = UUID.randomUUID().toString();
      instances.add(tempInstanceId);
      Future tempResponse = mock(Future.class);
      ResourceContext tempContext = mock(ResourceContext.class);
      contexts.add(tempContext);
      when(tempResponse.isDone()).thenReturn(true);
      // polling exception
      if (i == 0) {
        when(tempResponse.get()).thenReturn(new TaskResult(false, tempContext));
      } else {
        when(tempResponse.get()).thenReturn(new TaskResult(true, tempContext));
      }

      when(computeProviderHelper.submitVmCreationTask(tempContext, vnet, subnet, nsg, as, vmSize,
        vmNamePrefix, tempInstanceId, privateDomainName, userName, publicKey, dataDiskCount,
        imageInfo)).thenReturn(tempResponse);
    }

    when(credentials.createResourceContext(anyString(), anyString(), anyBoolean()))
      .thenReturn(contexts.get(0))
      .thenReturn(contexts.get(1))
      .thenReturn(contexts.get(2))
      .thenReturn(contexts.get(3));

    when(computeProviderHelper.pollPendingTasks(anySet(), anyInt(), anyInt(), anySet()))
      .thenCallRealMethod();

    ArgumentCaptor<Set> argumentCaptor = ArgumentCaptor.forClass(Set.class);
    computeProvider.allocate(template, instances, 3);

    verify(computeProviderHelper).deleteResources(anyString(), argumentCaptor.capture());
    for (int i = 0; i < 4; i++) {
      verify(computeProviderHelper).submitVmCreationTask(contexts.get(i), vnet, subnet, nsg, as,
        vmSize, vmNamePrefix, instances.get(i), privateDomainName, userName, publicKey,
        dataDiskCount, imageInfo);
    }
    assertEquals(expectedFailure, argumentCaptor.getValue().size());
    assertEquals(expectedSuccess, computeProvider.getLastSuccessfulAllocationCount());
  }

  @Test
  public void testAllocateLessThanRequiredExpectUnrecoverableProviderException()
    throws Exception {
    thrown.expect(UnrecoverableProviderException.class);

    when(computeProviderHelper.submitVmCreationTask(context, vnet, subnet, nsg, as, vmSize,
      vmNamePrefix, instanceId, privateDomainName, userName, publicKey, dataDiskCount, imageInfo))
      .thenReturn(response);

    when(computeProviderHelper.pollPendingTasks(anySet(), anyInt(), anyInt(), anySet()))
      .thenReturn(0);
    ArrayList list = new ArrayList();
    list.add(instanceId);

    try {
      computeProvider.allocate(template, list, 1);
    } catch (Exception e) {
      verify(computeProviderHelper).deleteResources(anyString(), anySet());
      throw e;
    }
  }

  @Test
  public void testRunningVMState() throws ServiceException, IOException, URISyntaxException {
    ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
    vms.add(vm);

    when(vm.getName()).thenReturn(vmName);

    when(computeProviderHelper.getVirtualMachines(resourceGroup)).thenReturn(vms);

    when(computeProviderHelper.getVirtualMachineStatus(resourceGroup, vmName))
      .thenReturn(new SimpleInstanceState(InstanceStatus.RUNNING));

    ArrayList<InstanceViewStatus> status = new ArrayList<InstanceViewStatus>();
    InstanceViewStatus ivs = new InstanceViewStatus();
    ivs.setCode(AzureVirtualMachineState.POWER_STATE_RUNNING);
    when(iv.getStatuses()).thenReturn(status);

    ArrayList list = new ArrayList();
    list.add(instanceId);
    Map<String, InstanceState> map = computeProvider.getInstanceState(template, list);
    assertEquals(1, map.size());

    for (InstanceState state : map.values()) {
      assertEquals(InstanceStatus.RUNNING, state.getInstanceStatus());
    }
  }

  @Test
  public void testDeletedVMState() throws ServiceException, IOException, URISyntaxException {
    //can't find delete VM, therefore, resource group maybe empty

    ArrayList<ResourceGroupExtended> groups = new ArrayList<ResourceGroupExtended>();
    ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
    ResourceGroupExtended group = mock(ResourceGroupExtended.class);
    groups.add(group);

    when(group.getName()).thenReturn(resourceGroup);

    when(computeProviderHelper.getResourceGroups()).thenReturn(groups);

    when(computeProviderHelper.getVirtualMachines(resourceGroup)).thenReturn(vms);

    ArrayList list = new ArrayList();
    list.add(instanceId);
    Map<String, InstanceState> map = computeProvider.getInstanceState(template, list);
    assertEquals(1, map.size());

    for (InstanceState state : map.values()) {
      assertEquals(InstanceStatus.UNKNOWN, state.getInstanceStatus());
    }

  }

  @Test
  public void testFoundByNameNotMatchedVMState()
    throws ServiceException, IOException, URISyntaxException {
    //can't find delete VM, therefore, resource group maybe empty

    ArrayList<ResourceGroupExtended> groups = new ArrayList<ResourceGroupExtended>();
    ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
    ResourceGroupExtended group = mock(ResourceGroupExtended.class);
    groups.add(group);
    vms.add(vm);

    when(group.getName()).thenReturn(resourceGroup);

    when(vm.getName()).thenReturn(instanceId);

    when(computeProviderHelper.getResourceGroups()).thenReturn(groups);

    when(computeProviderHelper.getVirtualMachines(resourceGroup)).thenReturn(vms);

    when(computeProviderHelper.getVirtualMachineStatus(resourceGroup, instanceId))
      .thenReturn(new SimpleInstanceState(InstanceStatus.RUNNING));

    ArrayList<InstanceViewStatus> status = new ArrayList<InstanceViewStatus>();
    InstanceViewStatus ivs = new InstanceViewStatus();
    ivs.setCode(AzureVirtualMachineState.POWER_STATE_RUNNING);
    when(iv.getStatuses()).thenReturn(status);

    ArrayList list = new ArrayList();
    list.add("WILLNOTFOUND");
    Map<String, InstanceState> map = computeProvider.getInstanceState(template, list);
    assertEquals(1, map.size());

    for (InstanceState state : map.values()) {
      assertEquals(InstanceStatus.UNKNOWN, state.getInstanceStatus());
    }
  }
}
