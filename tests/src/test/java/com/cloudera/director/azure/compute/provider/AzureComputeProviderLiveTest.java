package com.cloudera.director.azure.compute.provider;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.TestConfigHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.InstanceStatus;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.Launcher;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.ComputeManagementService;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterface;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.Configuration;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Live tests for exercising the Azure plugin through the Director SPI interface.
 * <p>
 * Testing these AzureComputeProvider.class / AbstractComputeProvider.class methods:
 * - allocate()
 * - find()
 * - getInstanceState()
 * - delete()
 */
public class AzureComputeProviderLiveTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void checkLiveTestFlag() {
    assumeTrue(TestConfigHelper.runLiveTests());
  }

  /**
   * This test is to stress the code path where there are multiple tasks running at the same time.
   *
   * @throws Exception
   */
  @Test
  public void spiInterfaces_allocateThenDeleteMultipleVMsAtTheSameTime_stressesMultithreadingCode()
    throws Exception {
    TestConfigHelper cfgHelper = new TestConfigHelper();
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
      cfgHelper.getProviderConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider
      .createResourceProvider(AzureComputeProvider.METADATA.getId(), cfgHelper.getProviderConfig());

    DefaultLocalizationContext defaultLocalizationContext = new DefaultLocalizationContext(
      Locale.getDefault(), "");
    HashMap<String, String> tags = new HashMap<String, String>();
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate("TestInstanceTemplate",
      cfgHelper.getProviderConfig(), tags, defaultLocalizationContext);

    // three is enough
    Collection<String> instances = new ArrayList<String>() {{
      add(UUID.randomUUID().toString());
      add(UUID.randomUUID().toString());
      add(UUID.randomUUID().toString());
    }};

    // create the VMs
    provider.allocate(template, instances, instances.size());

    // verify that all the instances can be found
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instances);
    assertEquals("Expected " + instances.size() + " instances to be found but " +
      foundInstances.size() + " were.", instances.size(), foundInstances.size());

    // verify that they were correctly allocated
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instances);
    assertEquals(instances.size(), instanceStates.size());
    for (String instance : instances) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // delete the VMs
    provider.delete(template, instances);

    // verify that all the instances were correctly deleted
    assertEquals(0, provider.find(template, instances).size());
  }

  @Test
  public void allocate_idempotentAllocateAndDelete_isIdempotent() throws Exception {
    TestConfigHelper cfgHelper = new TestConfigHelper();
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
      cfgHelper.getProviderConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
      AzureComputeProvider.METADATA.getId(), cfgHelper.getProviderConfig());

    DefaultLocalizationContext defaultLocalizationContext = new DefaultLocalizationContext(
      Locale.getDefault(), "");
    HashMap<String, String> tags = new HashMap<String, String>();
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate("TestInstanceTemplate",
      cfgHelper.getProviderConfig(), tags, defaultLocalizationContext);

    Collection<String> instances = new ArrayList<String>() {{
      add(UUID.randomUUID().toString());
    }};

    // create the VM the first time
    provider.allocate(template, instances, instances.size());

    // verify that the instance can be found
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instances);
    assertEquals("Expected " + instances.size() + " instances to be found but " +
      foundInstances.size() + " were.", instances.size(), foundInstances.size());

    // verify that it was correctly allocated
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instances);
    assertEquals(instances.size(), instanceStates.size());
    for (String instance : instances) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    boolean hitUnrecoverableProviderException = false;
    // try to create the same VM again - it won't create one because we find() the VM in the RG and
    // throw an UnrecoverableProviderException because we didn't create at least minCount VMs
    try {
      provider.allocate(template, instances, instances.size());
    } catch (UnrecoverableProviderException e) {
      hitUnrecoverableProviderException = true;
    }

    // verify that an UnrecoverableProviderException was thrown and caught
    assertTrue("An UnrecoverableProviderException was either not thrown or not caught",
      hitUnrecoverableProviderException);

    // verify that the instance can still be found
    foundInstances = provider.find(template, instances);
    assertEquals("Expected " + instances.size() + " instances to be found but " +
      foundInstances.size() + " were.", instances.size(), foundInstances.size());

    // verify that it is still correctly allocated
    instanceStates = provider.getInstanceState(template, instances);
    assertEquals(instances.size(), instanceStates.size());
    for (String instance : instances) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // delete the VM
    provider.delete(template, instances);

    // verify that all the instances were correctly deleted
    assertEquals(0, provider.find(template, instances).size());

    // try to delete the VM again - it won't because the VM isn't in the RG
    provider.delete(template, instances);

    // verify that there's still no instance
    assertEquals(0, provider.find(template, instances).size());
  }

  /**
   * This test verifies that on delete() we delete the public IP if the VM only if the template
   * specified that there was one (e.g. we won't delete a public IP that was manually attached).
   * It's done by creating a VM with a template that specifies a public IP, then deleting the same
   * VM with a different template that specifies no public IP. The public IP is then cleaned up
   * afterwards.
   *
   * @throws Exception
   */
  @Test
  public void spiInterfaces_allocateWithoutPublicIpThenAttachPublicIp_publicIpDoesNotGetDeleted()
    throws Exception {
    TestConfigHelper cfgHelper = new TestConfigHelper();
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
      cfgHelper.getProviderConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider
      .createResourceProvider(AzureComputeProvider.METADATA.getId(), cfgHelper.getProviderConfig());

    DefaultLocalizationContext defaultLocalizationContext = new DefaultLocalizationContext(
      Locale.getDefault(), "");
    HashMap<String, String> tags = new HashMap<String, String>();
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate("TestInstanceTemplate",
      cfgHelper.getProviderConfig(), tags, defaultLocalizationContext);

    // one VM is enough
    Collection<String> instances = new ArrayList<String>() {{
      add(UUID.randomUUID().toString());
    }};
    // create the VMs
    provider.allocate(template, instances, instances.size());
    // verify that the instances can be found
    Collection<AzureComputeInstance> foundInstances = provider.find(template, instances);
    assertEquals("Expected " + instances.size() + " instances to be found but " +
      foundInstances.size() + " were.", instances.size(), foundInstances.size());

    // verify that the instance was correctly allocated
    Map<String, InstanceState> instanceStates = provider.getInstanceState(template, instances);
    assertEquals(instances.size(), instanceStates.size());
    for (String instance : instances) {
      assertEquals(InstanceStatus.RUNNING, instanceStates.get(instance).getInstanceStatus());
    }

    // get the public IP for future cleanup
    AzureCredentials cred = cfgHelper.getAzureCredentials();
    Configuration config = cred.createConfiguration();
    AzureComputeProviderHelper computeProviderHelper = cred.getComputeProviderHelper();
    ComputeManagementClient computeManagementClient = ComputeManagementService.create(config);
    String vmName = template.getInstanceNamePrefix() + "-" + instances.toArray()[0];
    String rgName = TestConfigHelper.DEFAULT_TEST_RESOURCE_GROUP;

    VirtualMachine vm = computeManagementClient.getVirtualMachinesOperations().get(rgName, vmName)
      .getVirtualMachine();
    NetworkResourceProviderClient networkResourceProviderClient = NetworkResourceProviderService.
      create(config);
    NetworkInterface nic = networkResourceProviderClient
      .getNetworkInterfacesOperations()
      .get(rgName, computeProviderHelper.getNicNameFromVm(vm))
      .getNetworkInterface();

    // this plugin only attaches once nic
    NetworkInterfaceIpConfiguration ipConfiguration = nic.getIpConfigurations().get(0);
    String[] pipID = ipConfiguration.getPublicIpAddress().getId().split("/");
    String pipName = pipID[pipID.length - 1];

    // Create a new template and set et public IP to "No".
    HashMap<String, String> providerCfgMap = cfgHelper.getProviderCfgMap();
    providerCfgMap.put(
      AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey(), "No");
    AzureComputeInstanceTemplate templateWithoutPIP = new AzureComputeInstanceTemplate(
      "TestInstanceTemplate", new SimpleConfiguration(providerCfgMap), tags,
      defaultLocalizationContext);

    // delete the VM
    provider.delete(templateWithoutPIP, instances);
    // verify that the instance was correctly deleted
    assertEquals(0, provider.find(templateWithoutPIP, instances).size());

    // delete the public IP
    computeProviderHelper.beginDeletePublicIpAddressByName(rgName, pipName);
    // verify that the public IP was deleted
    exception.expect(ServiceException.class);
    networkResourceProviderClient.getPublicIpAddressesOperations().get(rgName, pipName);
  }

  /**
   * This test tries to allocate two instances with the same instanceIds at the same time.
   * It is purposefully ignored because:
   * 1. The current code does not have the expected behavior of leaving behind 1 happy VM and
   * resources. Instead it LEAKS a Network interface, a Public IP address, and a Storage account,
   * but no VM.
   * 2. Director ensures this will never happen
   *
   * @throws Exception
   */
  @Ignore("Director ensures this will never happen.")
  @Test
  public void allocate_twoOfTheSameInstanceIdsAtTheSameTime_doesnotLeakResources()
    throws Exception {
    TestConfigHelper cfgHelper = new TestConfigHelper();
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
      cfgHelper.getProviderConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
      AzureComputeProvider.METADATA.getId(), cfgHelper.getProviderConfig());

    DefaultLocalizationContext defaultLocalizationContext = new DefaultLocalizationContext(
      Locale.getDefault(), "");
    HashMap<String, String> tags = new HashMap<String, String>();
    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate("TestInstanceTemplate",
      cfgHelper.getProviderConfig(), tags, defaultLocalizationContext);

    // make a collection with two of the same instanceIds
    String instanceId = UUID.randomUUID().toString();
    Collection<String> instances = new ArrayList<String>();
    instances.add(instanceId);
    instances.add(instanceId);

    // create the VMs
    provider.allocate(template, instances, 1);

    // THIS TEST WILL FAIL AND LEAK RESOURCES - see the comment for this test
    assertEquals("THIS TEST WILL FAIL AND LEAK RESOURCES - Expected one of the instances to be " +
      "found, instead resources were leaked.", 1, provider.find(template, instances).size());
  }
}