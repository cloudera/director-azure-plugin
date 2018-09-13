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
 *  limitations under the License.
 *
 */

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.TestHelper.TEST_RESOURCE_GROUP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP;
import static com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty.VMSIZE;
import static com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationProperty.REGION;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate.SimpleResourceTemplateConfigurationPropertyToken.GROUP_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureInstance;
import com.cloudera.director.azure.compute.instance.VirtualMachineScaleSetVM;
import com.cloudera.director.azure.shaded.com.microsoft.azure.CloudException;
import com.cloudera.director.azure.shaded.com.microsoft.azure.PagedList;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.DataDisk;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSetVMs;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSets;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.msi.implementation.MSIManager;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.azure.shaded.rx.Observable;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Resource;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.CloudProvider;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VirtualMachineScaleSetAllocatorLiveTest extends AzureComputeProviderLiveTestBase {

  private final LocalizationContext context = new DefaultLocalizationContext(Locale.getDefault(), "");
  private final List<String> instanceIds = IntStream
      .range(0, 3)
      .mapToObj(i -> UUID.randomUUID().toString())
      .collect(Collectors.toList());
  private Azure azure = spy(super.azure);
  private AzureComputeProvider provider;
  private AzureComputeInstanceTemplate template;
  private Config azureConfig;

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    Config pluginConfig = AzurePluginConfigHelper.getAzurePluginConfig();
    Config azureProviderConfig = pluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
    azureConfig = spy(azureProviderConfig);
    Config spyConfig = spy(pluginConfig);
    doReturn(azureConfig).when(spyConfig).getConfig(eq(Configurations.AZURE_CONFIG_PROVIDER));
    setAzurePluginConfig(spyConfig);

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(PUBLIC_IP.unwrap().getConfigKey(), "No");
    map.put(GROUP_ID.unwrap().getConfigKey(), getFirstGroupOfUuid(UUID.randomUUID().toString()));
    SimpleConfiguration config = new SimpleConfiguration(map);

    CloudProvider cloudProvider = LAUNCHER.createCloudProvider(AzureCloudProvider.ID, config, Locale.getDefault());

    // spy on AzureComputeProvider
    provider = spy((AzureComputeProvider)
        cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
            TestHelper.buildValidDirectorLiveTestConfig()));

    VirtualMachineScaleSetAllocator allocator = new VirtualMachineScaleSetAllocator(
        azure, credentials.getMsiManager(), config::getConfigurationValue);

    doReturn(allocator)
        .when(provider)
        .createInstanceAllocator(
            anyBoolean(), any(Azure.class), any(GraphRbacManager.class), any(MSIManager.class), any(BiFunction.class));

    template = new AzureComputeInstanceTemplate("test-template", config, Collections.emptyMap(), context);
  }

  @After
  public void tearDown() throws InterruptedException {
    provider.delete(template, Collections.emptyList());
  }

  @Test
  public void testAllocateWithExpectedSizeSucceedAndFindSucceed() throws InterruptedException {
    Map<String, VirtualMachineScaleSetVM> instances = provider
        .allocate(template, instanceIds, instanceIds.size())
        .stream()
        .collect(Collectors.<AzureComputeInstance<?>, String, VirtualMachineScaleSetVM>toMap(
            Resource::getId, i -> (VirtualMachineScaleSetVM) i.unwrap()));

    assertThat(instances.size()).isEqualTo(instanceIds.size());

    for (VirtualMachineScaleSetVM vm : instances.values()) {
      assertThat(vm.regionName())
          .isEqualTo(template.getConfigurationValue(REGION, context));
      assertThat(vm.size())
          .isEqualTo(VirtualMachineSizeTypes.fromString(template.getConfigurationValue(VMSIZE, context)));
      assertThat(vm.storageProfile().imageReference().sku())
          .isEqualTo(Configurations.parseImageFromConfig(template, context).sku());
      assertThat(vm.storageProfile().imageReference().offer())
          .isEqualTo(Configurations.parseImageFromConfig(template, context).offer());
      assertThat(vm.storageProfile().imageReference().publisher())
          .isEqualTo(Configurations.parseImageFromConfig(template, context).publisher());
      assertThat(vm.storageProfile().dataDisks().size())
          .isEqualTo(Integer.parseInt(template.getConfigurationValue(DATA_DISK_COUNT, context)));
      assertThat(vm.getPublicIPAddress()).isNull();

      for (DataDisk disk : vm.storageProfile().dataDisks()) {
        assertThat(disk.diskSizeGB())
            .isEqualTo(Integer.parseInt(template.getConfigurationValue(DATA_DISK_SIZE, context)));
      }
    }

    provider
        .allocate(template, instanceIds, instanceIds.size())
        .stream()
        .map(instance -> instance.getId())
        .forEach(instanceId -> assertThat(instances.keySet().contains(instanceId)).isTrue());

    Map<String, InstanceState> states = provider.getInstanceState(template, instances.keySet());
    assertThat(states.size()).isEqualTo(instanceIds.size());

    provider
        .find(template, instances.keySet())
        .stream()
        .forEach(i -> assertThat(instances.get(i.getId()).name()).isEqualTo(i.unwrap().name()));

    assertThat(provider.find(template, Collections.emptyList()).size()).isEqualTo(instances.size());

    provider.delete(template, Collections.emptyList());
  }

  @Test
  public void testGrowVirtualMachineScaleSetSucceed() throws InterruptedException {
    Map<String, VirtualMachineScaleSetVM> instances = provider
        .allocate(template, instanceIds, instanceIds.size())
        .stream()
        .collect(Collectors.<AzureComputeInstance<?>, String, VirtualMachineScaleSetVM>toMap(
            Resource::getId, i -> (VirtualMachineScaleSetVM) i.unwrap()));

    assertThat(instances.size()).isEqualTo(instanceIds.size());

    Map<String, VirtualMachineScaleSetVM> updated = provider
        .allocate(
            template,
            Stream.concat(instanceIds.stream(), Stream.of(UUID.randomUUID().toString())).collect(Collectors.toList()),
            instanceIds.size() + 1)
        .stream()
        .collect(Collectors.<AzureComputeInstance<?>, String, VirtualMachineScaleSetVM>toMap(
            Resource::getId, i -> (VirtualMachineScaleSetVM) i.unwrap()));

    assertThat(updated.size()).isEqualTo(instances.size() + 1);
    assertThat(updated.keySet()).containsAll(instances.keySet());

    Map<String, InstanceState> states = provider.getInstanceState(template, updated.keySet());
    assertThat(states.size()).isEqualTo(updated.size());
  }

  @Test
  public void testInvalidCreateParameter() throws InterruptedException {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(PUBLIC_IP.unwrap().getConfigKey(), "No");
    map.put(GROUP_ID.unwrap().getConfigKey(), getFirstGroupOfUuid(UUID.randomUUID().toString()));
    map.put(DATA_DISK_SIZE.unwrap().getConfigKey(), "4096");
    SimpleConfiguration config = new SimpleConfiguration(map);

    AzureComputeInstanceTemplate instanceTemplate = new AzureComputeInstanceTemplate(
        "test-template", config, Collections.emptyMap(), context);
    try {
      provider.allocate(instanceTemplate, instanceIds, instanceIds.size());
      fail("Allocation expected to fail");
    } catch (UnrecoverableProviderException ex) {
      assertThat(ex.getCause()).isNotNull();
      assertThat(CloudException.class.isInstance(ex.getCause())).isTrue();
      assertThat(azure.virtualMachineScaleSets().getByResourceGroup(TEST_RESOURCE_GROUP, template.getGroupId()))
          .isNull();
    }

    Collection<? extends AzureComputeInstance<? extends AzureInstance>> instances =
        provider.allocate(instanceTemplate, instanceIds, 0);
    assertThat(instances).isEmpty();
  }

  @Test(expected = InterruptedException.class)
  public void testInterrupted() throws InterruptedException {
    VirtualMachineScaleSets virtualMachineScaleSets = spy(azure.virtualMachineScaleSets());
    doReturn(virtualMachineScaleSets).when(azure).virtualMachineScaleSets();
    doAnswer(
        invocation -> {
          Object obj = invocation.callRealMethod();
          Thread t = Thread.currentThread();
          Observable.timer(20, TimeUnit.SECONDS).subscribe(i -> t.interrupt());
          return obj;
        })
        .when(virtualMachineScaleSets)
        .define(anyString());

    provider.allocate(template, instanceIds, 0);
    fail("InterruptedException expected");
  }

  @Test
  public void testTimeoutWithInstanceLEMinCountProvisionedCleansUpAndThrows() throws InterruptedException {
    doReturn(30)
        .when(azureConfig)
        .getInt(eq(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS));

    VirtualMachineScaleSets virtualMachineScaleSets = spy(azure.virtualMachineScaleSets());
    doReturn(virtualMachineScaleSets).when(azure).virtualMachineScaleSets();

    VirtualMachineScaleSetVMs virtualMachines = mock(VirtualMachineScaleSetVMs.class);
    when(virtualMachines.list()).thenReturn(mock(PagedList.class, invocation -> Stream.empty()));
    VirtualMachineScaleSet vmss = mock(VirtualMachineScaleSet.class);
    when(vmss.virtualMachines()).thenReturn(virtualMachines);
    doReturn(Observable.just(vmss))
        .when(virtualMachineScaleSets)
        .getByIdAsync(anyString());

    try {
      provider.allocate(template, instanceIds, instanceIds.size());
    } catch (UnrecoverableProviderException e) {
      assertThat(RuntimeException.class.isInstance(e.getCause()));
      assertThat(azure.virtualMachineScaleSets().getByResourceGroup(TEST_RESOURCE_GROUP, template.getGroupId()))
          .isNull();
    }
  }

  @Test
  public void testTimeoutWithInstanceGTExpectedCountProvisioned() throws InterruptedException {
    doReturn(30)
        .when(azureConfig)
        .getInt(eq(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS));

    Collection<? extends AzureComputeInstance<? extends AzureInstance>> instances =
        provider.allocate(template, instanceIds, instanceIds.size());
    assertThat(instances.size()).isEqualTo(instanceIds.size());
  }

  private static void setAzurePluginConfig(Config config) throws NoSuchFieldException, IllegalAccessException {
    Field field = AzurePluginConfigHelper.class.getDeclaredField("azurePluginConfig");
    field.setAccessible(true);
    field.set(null, config);
  }
}
