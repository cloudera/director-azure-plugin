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

package com.cloudera.director.azure.compute.instance;

import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_INSTANCE;
import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_DISALLOWED_USERNAMES;
import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES;
import static com.cloudera.director.azure.TestConfigHelper.DEFAULT_TEST_V1_VM_SISES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.cloudera.director.azure.TestConfigHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.provider.AzureComputeProviderHelper;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.AvailabilitySet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.NetworkSecurityGroup;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.Subnet;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachineSize;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.models.VirtualMachineSizeListResponse;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.models.VirtualNetwork;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.models.ResourceGroupExtended;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.models.AccountType;
import com.cloudera.director.azure.shaded.com.microsoft.windowsazure.exception.ServiceException;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE;
import static org.mockito.Mockito.when;

/**
 * Simple test to verify AzureComputeInstanceTemplateConfigurationValidatorTest class.
 */
public class AzureComputeInstanceTemplateConfigurationValidatorTest {
  private AzureComputeInstanceTemplateConfigurationValidator validator;

  private AzureComputeProviderHelper helper;
  private VirtualMachineSizeListResponse virtualMachineSizeListResponse;
  private AzureCredentials credentials;
  private Configured defaultDirectorConfig; // config with default values
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext;

  // junit error strings
  private String vmValid = "VM type '%s' is valid (should be invalid).";
  private String vmInvalid = "VM type '%s' is invalid (should be valid).";
  private String prefixValid = "Instance name prefix '%s' is valid (should be invalid).";
  private String prefixInvalid = "Instance name prefix '%s' is invalid (should be valid).";
  private String storageAccountTypeValid = "Storage Account Type '%s' is valid (should be invalid)";
  private String storageAccountTypeInvalid = "Storage Account Type '%s' is invalid (should be valid)";
  private String sshUsernameValid = "SSH Username '%s' is valid (should be invalid).";
  private String sshUsernameInvalid = "SSH Username '%s' is invalid (should be valid).";
  private String suffixValid = "FQDN suffix '%s' is valid (should be invalid).";
  private String suffixInvalid = "FQDN suffix '%s' is invalid (should be valid).";
  private String vnValid = "Virtual Network '%s' is valid (should be invalid)";
  private String vnInvalid = "Virtual Network '%s' is invalid (should be valid)";
  private String subnetValid = "Subnet '%s' is valid (should be invalid)";
  private String subnetInvalid = "Subnet '%s' is invalid (should be valid)";
  private String nsgValid = "Network Security Group '%s' is valid (should be invalid)";
  private String nsgInvalid = "Network Security Group '%s' is invalid (should be valid)";
  private String asValid = "Availability Set '%s' is valid (should be invalid)";
  private String asInvalid = "Availability Set '%s' is invalid (should be valid)";
  private String rgValid = "Resource Group '%s' is valid (should be invalid)";
  private String rgInvalid = "Resource Group '%s' is invalid (should be valid)";
  private String locationValid = "Location for '%s' is valid (should be invalid)";
  private String locationInvalid = "Location for '%s' is invalid (should be valid)";

  private String rgName = "resourcegroup";
  private String vnrgName = "virtualnetworkresourcegroup";
  private String vnName = "virtualnetwork";
  private String subnetName = "default";
  private String nsgrgName = "networksecuritygroupresourcegroup";
  private String nsgName = "networksecuritygroup";
  private String asName = "availabilityset";
  private String vmSize = "STANDARD_DS14";
  private String locationEastUS = "eastus";
  private String locationWestUS = "westus";
  private String imageName = "cloudera-centos-6-latest";
  private ResourceGroupExtended rg = mock(ResourceGroupExtended.class);
  private ResourceGroupExtended vnrg = mock(ResourceGroupExtended.class);
  private VirtualNetwork vn = mock(VirtualNetwork.class);
  private Subnet subnet = mock(Subnet.class);
  private ResourceGroupExtended nsgrg = mock(ResourceGroupExtended.class);
  private NetworkSecurityGroup nsg = mock(NetworkSecurityGroup.class);
  private AvailabilitySet as = mock(AvailabilitySet.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    helper = mock(AzureComputeProviderHelper.class);
    TestConfigHelper.seedAzurePluginConfigWithDefaults();
    virtualMachineSizeListResponse = mock(VirtualMachineSizeListResponse.class);
    credentials = mock(AzureCredentials.class);

    when(credentials.getComputeProviderHelper()).thenReturn(helper);
    localizationContext = new DefaultLocalizationContext(Locale.getDefault(), "");
    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials, locationEastUS);
    validator = spy(validator);
    accumulator = new PluginExceptionConditionAccumulator();

    // Set the default values we use.
    // N.B.: some tests test changes to the default values; those tests don't use the
    // defaultDirectorConfig variable.
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("type", vmSize);
    cfgMap.put("hostFqdnSuffix", "cdh-cluster.internal");
    defaultDirectorConfig = spy(new SimpleConfiguration(cfgMap));
    // Always make sure that checking fields returns something so we can continue to the
    // `get___ByName` calls.
    doReturn(rgName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP, localizationContext);
    doReturn(rg)
      .when(helper)
      .getResourceGroup(rgName);

    doReturn(vnrgName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    doReturn(vnrg)
      .when(helper)
      .getResourceGroup(vnrgName);

    doReturn(vnName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK, localizationContext);
    doReturn(vn)
      .when(helper)
      .getVirtualNetworkByName(vnrgName, vnName);
    doReturn(locationEastUS)
      .when(vn)
      .getLocation();

    doReturn(subnetName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME, localizationContext);
    doReturn(subnet)
      .when(helper)
      .getSubnetByName(vnrgName, vnName, subnetName);

    doReturn(nsgrgName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    doReturn(nsgrg)
      .when(helper)
      .getResourceGroup(nsgrgName);

    doReturn(nsgName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        localizationContext);
    doReturn(nsg)
      .when(helper)
      .getNetworkSecurityGroupByName(nsgrgName, nsgName);
    doReturn(locationEastUS)
      .when(nsg)
      .getLocation();

    doReturn(asName)
        .when(defaultDirectorConfig)
        .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
          localizationContext);
    doReturn(as)
      .when(helper)
      .getAvailabilitySetByName(rgName, asName);
    doReturn(locationEastUS)
      .when(as)
      .getLocation();

    doReturn(vmSize)
      .when(defaultDirectorConfig)
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
        localizationContext);

    doReturn(imageName)
      .when(defaultDirectorConfig)
      .getConfigurationValue(IMAGE, localizationContext);

    // create the list of v1 VirtualMachineSizes
    ArrayList<VirtualMachineSize> v1VMs = new ArrayList<VirtualMachineSize>();
    for (String s : DEFAULT_TEST_V1_VM_SISES) {
      VirtualMachineSize vms = new VirtualMachineSize();
      vms.setName(s);
      v1VMs.add(vms);
    }
    doReturn(virtualMachineSizeListResponse)
      .when(helper)
      .getAvailableSizesInAS(rgName, asName);
    doReturn(v1VMs)
      .when(virtualMachineSizeListResponse)
      .getVirtualMachineSizes();
  }

  @After
  public void tearDown() throws Exception {
    helper = null;
    localizationContext = null;
    validator = null;
    accumulator = null;
    defaultDirectorConfig = null;
  }

  //
  // This is test calls AzureComputeInstanceTemplateConfigurationValidator.validate()
  //

  @Test
  public void validate_validInput_success() throws Exception {
    validator.validate(null, defaultDirectorConfig, accumulator, localizationContext);

    assertEquals("Something is invalid (everything should be valid).",
      0, accumulator.getConditionsByKey().size());
    verify(validator, times(1))
      .checkVMSize(defaultDirectorConfig, accumulator, localizationContext);
    verify(validator, times(1))
      .checkFQDNSuffix(defaultDirectorConfig, accumulator, localizationContext);
    verify(validator, times(1))
      .checkInstancePrefix(defaultDirectorConfig, accumulator, localizationContext);
    verify(validator, times(1))
      .checkStorage(defaultDirectorConfig, accumulator, localizationContext);
    verify(validator, times(1))
      .checkSshUsername(defaultDirectorConfig, accumulator, localizationContext);
    verify(validator, times(1))
      .checkResourceGroup(defaultDirectorConfig, accumulator, localizationContext, helper);
    verify(validator, times(1))
      .checkVirtualNetworkResourceGroup(defaultDirectorConfig, accumulator, localizationContext,
        helper);
    verify(validator, times(1))
      .checkVirtualNetwork(defaultDirectorConfig, accumulator, localizationContext, helper);
    verify(validator, times(1))
      .checkSubnet(defaultDirectorConfig, accumulator, localizationContext, helper);
    verify(validator, times(1))
      .checkNetworkSecurityGroupResourceGroup(defaultDirectorConfig, accumulator,
        localizationContext, helper);
    verify(validator, times(1))
      .checkNetworkSecurityGroup(defaultDirectorConfig, accumulator, localizationContext, helper);
    verify(validator, times(1))
      .checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);
    verify(validator, times(1))
      .checkVmImage(defaultDirectorConfig, accumulator, localizationContext, helper);
  }

  @Test
  public void validate_UnknownException() throws Exception {
    when(helper.getMarketplaceVMImage(anyString(), any(AzureVmImageInfo.class)))
      .thenThrow(new RuntimeException());
    validator.validate(null, defaultDirectorConfig, accumulator, localizationContext);

    assertEquals("Should catch and log generic exception.",
      1, accumulator.getConditionsByKey().size());
  }

  //
  // Virtual Machine Size Tests
  //

  @Test
  public void checkVMSize_defaultInput_success() throws Exception {
    final List<String> vmTypes = new ArrayList<String>() {{
      add("STANDARD_DS13");
      add("STANDARD_DS14");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String vmType : vmTypes) {
      cfgMap.put("type", vmType);
      validator.checkVMSize(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(vmInvalid, vmType), 0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkVMSize_invalidInput_error() throws Exception {
    final List<String> vmTypes = new ArrayList<String>() {{
      add("STANDARD_DS3");
      add("STANDARD_DS4");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String vmType : vmTypes) {
      cfgMap.put("type", vmType);
      validator.checkVMSize(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(vmValid, vmType), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  //
  // Resource Group Tests
  //

  @Test
  public void checkResourceGroup_validInput_success() throws Exception {
    validator.checkResourceGroup(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(rgInvalid, rgName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkResourceGroup_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkResourceGroup(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(rgInvalid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP, localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkResourceGroup_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkResourceGroup(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(rgInvalid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkResourceGroup_invalidInput_URISyntaxException() throws Exception {
    doThrow(new URISyntaxException("", ""))
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkResourceGroup(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(rgInvalid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  //
  // Virtual Network Resource Group Tests
  //

  @Test
  public void checkVirtualNetworkResourceGroup_validInput_success() throws Exception {
    validator.checkVirtualNetworkResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgInvalid, vnrgName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkVirtualNetworkResourceGroup_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkVirtualNetworkResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkVirtualNetworkResourceGroup_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkVirtualNetworkResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkVirtualNetworkResourceGroup_invalidInput_URISyntaxException() throws Exception {
    doThrow(new URISyntaxException("", ""))
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkVirtualNetworkResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, rgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  //
  // Virtual Network Tests
  //

  @Test
  public void checkVirtualNetwork_validInput_success() throws Exception {
    validator.checkVirtualNetwork(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(vnInvalid, vnName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getVirtualNetworkByName(anyString(), anyString());
    verify(vn, times(1)).getLocation();
  }

  @Test
  public void checkVirtualNetwork_invalidLocation_error() throws Exception {
    doReturn(locationWestUS)
      .when(vn)
      .getLocation();

    validator.checkVirtualNetwork(defaultDirectorConfig, accumulator, localizationContext, helper);
    assertEquals(String.format(locationValid, vnName), 1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVirtualNetwork_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getVirtualNetworkByName(anyString(), anyString());

    validator.checkVirtualNetwork(defaultDirectorConfig, accumulator, localizationContext, helper);
    assertEquals(String.format(vnValid, vnName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getVirtualNetworkByName(anyString(), anyString());
  }

  @Test
  public void checkVirtualNetwork_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getVirtualNetworkByName(anyString(), anyString());

    validator.checkVirtualNetwork(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(vnValid, vnName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getVirtualNetworkByName(anyString(), anyString());
  }

  //
  // Host FQDN Suffix Tests
  //

  @Test
  public void checkFQDNSuffix_defaultInput_success() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      add("cdh-cluster.internal"); // default works
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixInvalid, suffix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffix_validLengths_success() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      // It can be >= 1 character
      add("a");
      add("1");
      add("ab");
      add("12");
      add("abc");
      add("123");

      // It can be <= 37 characters
      add("aaaaaaaaaaaaaaaa.cdh-cluster.internal"); // 37 characters
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixInvalid, suffix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffix_invalidLengths_error() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      // It cannot be < 1 characters
      add("");

      // It cannot be > 37 characters
      add("aaaaaaaaaaaaaaaaa.cdh-cluster.internal"); // 38 characters
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("type", "STANDARD_DS14");

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixValid, suffix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffix_hyphensAndNumbersAndDotsInValidPositions_success() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      add("aaaa"); // only letters
      add("1111"); // only numbers
      add("a-a"); // contains a hyphen
      add("a-----a"); // contains multiple hyphens
      add("a1a"); // contains a number
      add("a111111a"); // contains multiple numbers
      add("1aa"); // starts with a number
      add("a1"); // ends with a number
      add("aa1"); // ends with a number
      add("aa111111"); // ends with multiple numbers
      add("abcde.a.abcde"); // length of 1
      add("abcde.ab.abcde"); // length of 2
      add("abcde.abc.abcde"); // length of 3
      add("abcde.1.abcde"); // only numbers, length of 1
      add("abcde.12.abcde"); // only numbers, length of 2
      add("abcde.123.abcde"); // only numbers, length of 3
      add("abcd.1abc.abcd"); // starts with number
      add("aaa-bbb-ccc-ddd-eee-fff-ggg-hhh-iii"); // contains lots of hyphens (-)
      add("aaa.bbb.ccc.ddd.eee.fff.ggg.hhh.iii"); // contains lots of dots (.)
      add("aaa.bb2.c3c.d44.e-e5.f--f.ggg.hhh.iii"); // contains lots of everything
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixInvalid, suffix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffix_hyphensAndNumbersAndDotsInInvalidPositions_error() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      // It can contain only lowercase letters, numbers and hyphens.
      // The first character must be a letter.
      // The last character must be a letter or number.
      add("-abc"); // starts with hyphen
      add("abc-"); // ends with hyphen
      add("ABC"); // not lowercase
      add("aBc"); // not lowercase
      add("Abc"); // not lowercase
      add("abC"); // not lowercase

      // Same as above, but wrapped with valid labels.
      add("abcd..abcd"); // empty
      add("abcd.-abc.abcd"); // starts with hyphen
      add("abcd.abc-.abcd"); // ends with hyphen
      add("abcd.ABC.abcd"); // not lowercase
      add("abcd.aBc.abcd"); // not lowercase
      add("abcd.Abc.abcd"); // not lowercase
      add("abcd.abC.abcd"); // not lowercase

      // Cannot start or end with dot
      add(".abc");
      add("abc.");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixValid, suffix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffix_invalidCharacters_error() throws Exception {
    final List<String> suffixes = new ArrayList<String>() {{
      // It cannot contain the following characters:
      // ` ~ ! @ # $ % ^ & * ( ) = + _ [ ] { } \ | ; : ' " , < > / ?
      add("ab`cd");
      add("ab~cd");
      add("ab!cd");
      add("ab@cd");
      add("ab#cd");
      add("ab$cd");
      add("ab%cd");
      add("ab^cd");
      add("ab&cd");
      add("ab*cd");
      add("ab(cd");
      add("ab)cd");
      add("ab=cd");
      add("ab+cd");
      add("ab_cd");
      add("ab[cd");
      add("ab]cd");
      add("ab{cd");
      add("ab}cd");
      add("ab\\cd");
      add("ab|cd");
      add("ab;cd");
      add("ab:cd");
      add("ab'cd");
      add("ab\"cd");
      add("ab,cd");
      add("ab<cd");
      add("ab>cd");
      add("ab/cd");
      add("ab?cd");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String suffix : suffixes) {
      cfgMap.put("hostFqdnSuffix", suffix);
      validator.checkFQDNSuffix(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(suffixValid, suffix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  //
  // Network Security Group Resource Group Tests
  //

  @Test
  public void checkNetworkSecurityGroupResourceGroup_validInput_success() throws Exception {
    validator.checkNetworkSecurityGroupResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(nsgInvalid, nsgrgName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkNetworkSecurityGroupResourceGroup_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkNetworkSecurityGroupResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, nsgrgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkNetworkSecurityGroupResourceGroup_invalidInput_ServiceException()
    throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkNetworkSecurityGroupResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, nsgrgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  @Test
  public void checkNetworkSecurityGroupResourceGroup_invalidInput_URISyntaxException()
    throws Exception {
    doThrow(new URISyntaxException("", ""))
      .when(helper)
      .getResourceGroup(anyString());

    validator.checkNetworkSecurityGroupResourceGroup(defaultDirectorConfig, accumulator,
      localizationContext, helper);

    assertEquals(String.format(rgValid, nsgrgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getResourceGroup(anyString());
  }

  //
  // Network Security Group Tests
  //

  @Test
  public void checkNetworkSecurityGroup_validInput_success() throws Exception {
    validator.checkNetworkSecurityGroup(defaultDirectorConfig, accumulator, localizationContext,
      helper);

    assertEquals(String.format(nsgInvalid, nsgName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getNetworkSecurityGroupByName(anyString(), anyString());
    verify(nsg, times(1)).getLocation();
  }

  @Test
  public void checkNetworkSecurityGroup_invalidLocation_error() throws Exception {
    doReturn(locationWestUS)
      .when(nsg)
      .getLocation();

    validator.checkNetworkSecurityGroup(defaultDirectorConfig, accumulator, localizationContext,
      helper);
    assertEquals(String.format(locationValid, nsgName), 1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkSecurityGroup_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getNetworkSecurityGroupByName(anyString(), anyString());

    validator.checkNetworkSecurityGroup(defaultDirectorConfig, accumulator, localizationContext,
      helper);

    assertEquals(String.format(nsgValid, nsgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP, localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getNetworkSecurityGroupByName(anyString(), anyString());
  }

  @Test
  public void checkNetworkSecurityGroup_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getNetworkSecurityGroupByName(anyString(), anyString());

    validator.checkNetworkSecurityGroup(defaultDirectorConfig, accumulator, localizationContext,
      helper);

    assertEquals(String.format(nsgValid, nsgName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(0))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getNetworkSecurityGroupByName(anyString(), anyString());
  }

  //
  // Availability Set Tests
  //

  @Test
  public void checkAvailabilitySet_validInput_success() throws Exception {
    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext,
      helper);

    assertEquals(String.format(asInvalid, asName), 0, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
        localizationContext);
    verify(as, times(1)).getLocation();
  }

  @Test
  public void checkAvailabilitySet_invalidLocation_error() throws Exception {
    doReturn(locationWestUS)
      .when(as)
      .getLocation();

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext,
      helper);
    assertEquals(String.format(locationValid, asName), 1, accumulator.getConditionsByKey().size());
  }

    @Test
  public void checkAvailabilitySet_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getAvailabilitySetByName(anyString(), anyString());

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asValid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
  }

  @Test
  public void checkAvailabilitySet_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getAvailabilitySetByName(anyString(), anyString());

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asValid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
  }

  @Test
  public void checkAvailabilitySet_invalidInput_URISyntaxException() throws Exception {
    doThrow(new URISyntaxException("", ""))
      .when(helper)
      .getAvailabilitySetByName(anyString(), anyString());

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asValid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
  }

  @Test
  public void checkAvailabilitySet_invalidVMSize_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getAvailableSizesInAS(anyString(), anyString());

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asValid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
    verify(helper, times(1))
      .getAvailableSizesInAS(anyString(), anyString());
  }


  @Test
  public void checkAvailabilitySet_invalidVMSize_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getAvailableSizesInAS(anyString(), anyString());

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asValid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
    verify(helper, times(1))
      .getAvailableSizesInAS(anyString(), anyString());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
        localizationContext);
  }

  @Test
  public void checkAvailabilitySet_invalidVMSize_v2VMv1AS_error() throws Exception {
    // v2 VM with an AS that only allows v1 VM sizes
    doReturn("STANDARD_DS14_V2")
      .when(defaultDirectorConfig)
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
        localizationContext);

    validator.checkAvailabilitySet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(asInvalid, asName), 1, accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getAvailabilitySetByName(anyString(), anyString());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE,
        localizationContext);
  }

  //
  // Instance Name Prefix Tests
  //

  @Test
  public void checkInstancePrefix_defaultInput_success() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      add("director"); // default
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("type", "STANDARD_DS14");

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixInvalid, prefix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefix_validLengths_success() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      // It can be > 3 characters
      add("aaa");

      // It can be <= 17 characters
      add("aaaaaaaaaaaaaaaaa"); // 16 characters

    }};

    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("type", "STANDARD_DS14");

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixInvalid, prefix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefix_invalidLengths_error() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      // It cannot be < 3 characters
      add("");
      add("a");
      add("aa");

      // It cannot be > 17 characters
      add("aaaaaaaaaaaaaaaaaa"); // 18 characters

    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixValid, prefix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefix_hyphensAndNumbersInValidPositions_success() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      // hyphens
      add("a-a");
      add("aaa-aaa-aaa-aaa");
      add("a-----a");

      // numbers
      add("a1a");
      add("a111111a");
      add("aaa1aaa1aaa1aaa");
      add("aa1");
      add("aaa1aaa1aaa1aaa1");
      add("aa111111");

      // both
      add("a-1");
      add("a-a1-1-a11");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("type", "STANDARD_DS14");

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixInvalid, prefix),
        0, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefix_hyphensAndNumbersInInvalidPositions_error() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      // It can contain only lowercase letters, numbers and hyphens.
      // The first character must be a letter.
      // The last character must be a letter or number.
      add("1abc"); // starts with number
      add("-abc"); // starts with hyphen
      add("abc-"); // ends with hyphen
      add("ABC"); // not lowercase
      add("aBc"); // not lowercase
      add("Abc"); // not lowercase
      add("abC"); // not lowercase
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixValid, prefix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefix_invalidCharacters_error() throws Exception {
    final List<String> prefixes = new ArrayList<String>() {{
      // It cannot contain the following characters:
      // ` ~ ! @ # $ % ^ & * ( ) = + _ [ ] { } \ | ; : ' " , < > / ?
      add("ab`cd");
      add("ab~cd");
      add("ab!cd");
      add("ab@cd");
      add("ab#cd");
      add("ab$cd");
      add("ab%cd");
      add("ab^cd");
      add("ab&cd");
      add("ab*cd");
      add("ab(cd");
      add("ab)cd");
      add("ab=cd");
      add("ab+cd");
      add("ab_cd");
      add("ab[cd");
      add("ab]cd");
      add("ab{cd");
      add("ab}cd");
      add("ab\\cd");
      add("ab|cd");
      add("ab;cd");
      add("ab:cd");
      add("ab'cd");
      add("ab\"cd");
      add("ab,cd");
      add("ab<cd");
      add("ab>cd");
      add("ab/cd");
      add("ab?cd");
    }};

    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String prefix : prefixes) {
      cfgMap.put("instanceNamePrefix", prefix);
      validator.checkInstancePrefix(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext);
      assertEquals(String.format(prefixValid, prefix), 1, accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  //
  // Storage tests
  //
  @Test
  public void checkStorage_validPremiumStorageAccountType_validSize_success() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.PremiumLRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    final List<String> diskSizes = new ArrayList<String>() {{
      add("512");
      add("1023");
    }};

    for (String diskSize : diskSizes) {
      cfgMap.put("dataDiskSize", diskSize);

      validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(storageAccountTypeInvalid, storageAccountType), 0,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorage_validPremiumStorageAccountType_invalidSize_error() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.PremiumLRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    final List<String> diskSizes = new ArrayList<String>() {{
      add("-1");
      add("0");
      add("511");
      add("513");
      add("1024");
      add("2048");
    }};

    for (String diskSize : diskSizes) {
      cfgMap.put("dataDiskSize", diskSize);

      validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(storageAccountTypeInvalid, storageAccountType), 1,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorage_validStandardStorageAccountType_validSize_success() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.StandardLRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    final List<String> diskSizes = new ArrayList<String>() {{
      add("1");
      add("512");
      add("1023");
    }};

    for (String diskSize : diskSizes) {
      cfgMap.put("dataDiskSize", diskSize);

      validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(storageAccountTypeInvalid, storageAccountType), 0,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorage_validStandardStorageAccountType_invalidSize_error() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.StandardLRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    final List<String> diskSizes = new ArrayList<String>() {{
      add("-1");
      add("0");
      add("1024");
      add("2048");
    }};
    for (String diskSize : diskSizes) {
      cfgMap.put("dataDiskSize", diskSize);

      validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(storageAccountTypeValid, storageAccountType), 1,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorage_invalidStorageAccountType_validSize_error()
    throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.StandardRAGRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    cfgMap.put("dataDiskSize", "512");

    validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
    // only 1 error because we don't validate sizes on storage account types not in the default list
    assertEquals(String.format(storageAccountTypeValid, storageAccountType), 1,
      accumulator.getConditionsByKey().size());
    accumulator.getConditionsByKey().clear();
  }

  @Test
  public void checkStorage_nonDefaultStorageAccountType_anySize_success() throws Exception {
    // build a custom config with only the storage accounts to test
    final List<String> accountTypeConfig = new ArrayList<String>() {{
      add(AccountType.PremiumLRS.toString());
      add(AccountType.StandardLRS.toString());
      add(AccountType.StandardRAGRS.toString());
    }};
    Map<String, Map> config = new HashMap<String, Map>();
    config.put(AZURE_CONFIG_INSTANCE, new HashMap<String, List<String>>());
    config.get(AZURE_CONFIG_INSTANCE).put(AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES, accountTypeConfig);
    AzurePluginConfigHelper.mergeAzurePluginConfig(ConfigFactory.parseMap(config));

    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials, locationEastUS);
    validator = spy(validator);

    // do the test
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = AccountType.StandardRAGRS.toString();
    cfgMap.put("storageAccountType", storageAccountType);
    final List<String> diskSizes = new ArrayList<String>() {{
      add("0");
      add("1");
      add("512");
      add("1023");
      add("1024");
      add("2048");
    }};

    for (String diskSize : diskSizes) {
      cfgMap.put("dataDiskSize", diskSize);

      validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(storageAccountTypeInvalid, storageAccountType), 0,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorage_nonexistentStorageAccountType_error() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String storageAccountType = "this_is_not_a_storage_account_type";
    cfgMap.put("storageAccountType", storageAccountType);

    validator.checkStorage(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
    assertEquals(String.format(storageAccountTypeValid, storageAccountType), 1,
      accumulator.getConditionsByKey().size());
    accumulator.getConditionsByKey().clear();
  }

  //
  // SSH Username tests
  //

  @Test
  public void checkSSHUsername_validUsername_success() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    String allowedUsername = "cloudera";
    cfgMap.put("sshUsername", allowedUsername);
    validator.checkSshUsername(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
    assertEquals(String.format(sshUsernameInvalid, allowedUsername), 0,
      accumulator.getConditionsByKey().size());
    accumulator.getConditionsByKey().clear();
  }

  @Test
  public void checkSSHUsername_disallowedUsername_error() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();

    for (String disallowedUsername : AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
      .getStringList(AZURE_CONFIG_DISALLOWED_USERNAMES)) {
      cfgMap.put("sshUsername", disallowedUsername);
      validator.checkSshUsername(new SimpleConfiguration(cfgMap), accumulator, localizationContext);
      assertEquals(String.format(sshUsernameValid, disallowedUsername), 1,
        accumulator.getConditionsByKey().size());
      accumulator.getConditionsByKey().clear();
    }
  }

  //
  // VM image tests
  //

  @Test
  public void testInvalidImageName() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("image", "foobarImage");
    SimpleConfiguration dirCfg = new SimpleConfiguration(cfgMap);

    validator.checkVmImage(dirCfg, accumulator, localizationContext, helper);

    assertEquals("Invalid image name should be detected", 1,
      accumulator.getConditionsByKey().size());
  }

  @Test
  public void testNonExistentImage() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getMarketplaceVMImage(anyString(), any(AzureVmImageInfo.class));

    validator.checkVmImage(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals("Non-existent VM image should be caught", 1,
      accumulator.getConditionsByKey().size());
  }

  @Test
  public void testImagePermissionProblemWithIllegalArgumentException() throws Exception {
    doThrow(new IllegalArgumentException())
      .when(helper)
      .getMarketplaceVMImage(anyString(), any(AzureVmImageInfo.class));

    validator.checkVmImage(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals("Problematic permission VM image with IllegalArgument Exception should be " +
      "caught", 1,
      accumulator.getConditionsByKey().size());
  }

  //
  // Subnet tests
  //

  @Test
  public void checkSubnet_validInput_Success() throws Exception {
    validator.checkSubnet(defaultDirectorConfig, accumulator, localizationContext, helper);

    assertEquals(String.format(subnetValid, subnetName), 0,
      accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getSubnetByName(anyString(), anyString(), anyString());
  }

  @Test
  public void checkSubnet_invalidInput_IOException() throws Exception {
    doThrow(new IOException())
      .when(helper)
      .getSubnetByName(anyString(), anyString(), anyString());

    validator.checkSubnet(defaultDirectorConfig, accumulator, localizationContext, helper);
    assertEquals(String.format(subnetValid, subnetName), 1,
      accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getSubnetByName(anyString(), anyString(), anyString());
  }

  @Test
  public void checkSubnet_invalidInput_ServiceException() throws Exception {
    doThrow(new ServiceException())
      .when(helper)
      .getSubnetByName(anyString(), anyString(), anyString());

    validator.checkSubnet(defaultDirectorConfig, accumulator, localizationContext, helper);
    assertEquals(String.format(subnetValid, subnetName), 1,
      accumulator.getConditionsByKey().size());
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME,
        localizationContext);
    verify(defaultDirectorConfig, times(1))
      .getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP,
        localizationContext);
    verify(helper, times(1))
      .getSubnetByName(anyString(), anyString(), anyString());
  }
}
