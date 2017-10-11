/*
 * Copyright (c) 2017 Cloudera, Inc.
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

import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.CustomVmImageTestHelper;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.provider.Launcher;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * AzureComputeInstanceTemplateConfigurationValidator live tests for checks that require interacting
 * with the Azure backend.
 *
 * Scenarios purposefully not tested:
 * - missing template fields that are required: Director does an initial template validation pass
 *   that will catch missing fields
 */
public class AzureComputeInstanceTemplateConfigurationValidatorLiveTest {

  // Fields used by checks
  private static AzureCredentials credentials;
  private static Azure azure;
  private AzureComputeInstanceTemplateConfigurationValidator validator;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext;

  @BeforeClass
  public static void createLiveTestResources() {
    Assume.assumeTrue(TestHelper.runLiveTests());

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf

    // initialize everything after the live test check
    credentials = TestHelper.getAzureCredentials();
    azure = credentials.authenticate();
    TestHelper.buildLiveTestEnvironment(azure);
  }

  @Before
  public void setUp() throws Exception {
    // Reset the plugin config with the default config.
    AzurePluginConfigHelper.setAzurePluginConfig(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME));
    AzurePluginConfigHelper.setConfigurableImages(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE));

    accumulator = new PluginExceptionConditionAccumulator();
    localizationContext = new DefaultLocalizationContext(Locale.getDefault(), "");

    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials,
        TestHelper.TEST_REGION);
  }

  @After
  public void reset() throws Exception {
    accumulator.getConditionsByKey().clear();

    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }
  @AfterClass
  public static void destroy() throws Exception {
    // this method is always called

    // destroy everything only if live check passes
    if (TestHelper.runLiveTests()) {
      TestHelper.destroyLiveTestEnvironment(azure);
    }
  }

  @Test
  public void validateWithLiveTestFieldsExpectNoErrors() throws Exception {
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf

    validator.validate(null, TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkComputeRgWithNonExistentComputeRgExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    validator.checkComputeResourceGroup(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkWithNothingExistsExpectAccumulatesOneErrorAndShortCircuits()
      throws Exception {
    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials, "fake-region");

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP
        .unwrap().getConfigKey(), "fake-virtual-network-resource-group");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK.unwrap()
        .getConfigKey(), "fake-virtual-network");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME.unwrap().getConfigKey(),
        "fake-subnet");

    validator.checkNetwork(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkWithNonexistentVirtualNetworkRgExpectAccumulatesOneErrorAndShortCircuits()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP
        .unwrap().getConfigKey(), "fake-virtual-network-resource-group");

    validator.checkNetwork(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkWithNonexistentVirtualNetworkExpectAccumulatesOneErrorAndShortCircuits()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK.unwrap()
        .getConfigKey(), "fake-virtual-network");

    validator.checkNetwork(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkWithVirtualNetworkNotInRegionExpectAccumulatesOneErrorAndShortCircuits()
      throws Exception {
    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials, "fake-region");

    validator.checkNetwork(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkWithNonexistentSubnetExpectAccumulatesOneErrorAndShortCircuits()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME.unwrap().getConfigKey(),
        "fake-subnet");

    validator.checkNetwork(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkSecurityGroupRgWithNonexistentNsgRgExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP
        .unwrap().getConfigKey(), "fake-network-security-group-resource-group");

    validator.checkNetworkSecurityGroupResourceGroup(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkSecurityGroupWithNonexistentNsgRgExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP
        .unwrap().getConfigKey(), "fake-network-security-group-resource-group");

    validator.checkNetworkSecurityGroup(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkNetworkSecurityGroupWithNonexistentNsgExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP.unwrap()
        .getConfigKey(), "fake-network-security-group");

    validator.checkNetworkSecurityGroup(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithNonexistentComputeRgExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "fake-compute-resource-group");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAvailabilitySetNotInRegionExpectAccumulatesErrors() throws Exception {
    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials, "fake-region");

    validator.checkAvailabilitySetAndManagedDisks(TestHelper.buildValidDirectorLiveTestConfig(),
        accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAvailabilitySetVmMismatchExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE.unwrap()
        .getConfigKey(), "fake-vm-size");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAsAndMdCompatibleExpectNoErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_MANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "Yes");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAsAndSaCompatibleExpectNoErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "No");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAsAndMdIncompatibleExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_MANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "No");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(2, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkAsAndMdWithAsAndSaIncompatibleExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TestHelper.TEST_AVAILABILITY_SET_UNMANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "Yes");

    validator.checkAvailabilitySetAndManagedDisks(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(2, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidImageLatestVersionExpectNoErrors() throws Exception {
    validator.checkVmImage(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidUriLatestVersionExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os/sku/7_2/version/latest");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidUriSpecificVersionExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os/sku/6_7/version/1.0.0");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidImageSpecificVersionExpectNoErrors() throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get(TestHelper.TEST_CENTOS_IMAGE_NAME))
        .put("version", "1.0.0");
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    validator.checkVmImage(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithPreviewImageExpectNoErrors() throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get("cloudera-centos-72-latest"))
        .put("offer", "cloudera-centos-os-preview");
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "cloudera-centos-72-latest");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  @Ignore
  public void checkVmImageWithValidUriPreviewLatestVersionExpectNoError() throws Exception {
    // FIXME https://github.com/Azure/azure-sdk-for-java/issues/1890

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/latest");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  @Ignore
  public void checkVmImageWithValidUriPreviewSpecificVersionExpectNoError() throws Exception {
    // FIXME https://github.com/Azure/azure-sdk-for-java/issues/1890

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/1.0.0");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  @Ignore
  public void checkVmImageWithValidUriPreviewInvalidSpecificVersionExpectAccumulatesErrors() throws Exception {
    // FIXME https://github.com/Azure/azure-sdk-for-java/issues/1890

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os-preview/sku/7_2/version/99.99.99");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidImageNonexistentSpecificVersionExpectAccumulatesErrors()
      throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get(TestHelper.TEST_CENTOS_IMAGE_NAME))
        .put("version", "0.0.0");
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    validator.checkVmImage(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidUriInvalidSpecificVersionExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/cloudera/offer/cloudera-centos-os/sku/7_2/version/0.0.0");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithImageMissingInConfigExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "fake-image");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithImageConfigMissingFieldsExpectAccumulatesErrors() throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get(TestHelper.TEST_CENTOS_IMAGE_NAME)).clear();
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    validator.checkVmImage(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithImageMissingInAzureExpectAccumulatesErrors() throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get(TestHelper.TEST_CENTOS_IMAGE_NAME)).put("version",
        "fake-version");
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    validator.checkVmImage(TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidUriInvalidFieldsAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/publisher/fake-publisher/offer/fake-offer/sku/fake-sku/version/fake-version");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkCustomImageValidInputsExpectSuccess() throws Exception {
    // Create a valid custom image for testing
    String customImageName = "test-custom-image-" + UUID.randomUUID();
    String customImageUri = CustomVmImageTestHelper.buildCustomManagedVmImage(
        customImageName, false);

    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        customImageUri);

    validator.checkUseCustomImage(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());

    CustomVmImageTestHelper.deleteCustomManagedVmImage(customImageUri);
  }

  @Test
  public void checkCustomImageInvalidUriExpectError() throws Exception {
    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");

    validator.checkUseCustomImage(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkCustomImageDoesNotExistExpectError() throws Exception {
    final String dummyCustomImageId = "/subscriptions/" + UUID.randomUUID() +
        "/resourceGroups/dummyRg/providers/Microsoft.Compute/images/dummyImageName";
    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        dummyCustomImageId);

    validator.checkUseCustomImage(new SimpleConfiguration(cfgMap), accumulator,
        localizationContext, azure);

    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkImplicitMsiAadGroupExpectNoErrors() throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
            .unwrap().getConfigKey(),
        TestHelper.TEST_AAD_GROUP_NAME);

    validator.checkImplicitMsiGroupName(new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkImplicitMsiNoAadGroupExpectNoErrors() throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), "Yes");

    validator.checkImplicitMsiGroupName(new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkImplicitMsiEmptyAadGroupExpectNoErrors() throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
            .unwrap().getConfigKey(), "");

    validator.checkImplicitMsiGroupName(new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void skipCheckImplicitMsiAadGroupExpectNoErrors() throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), "No");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
            .unwrap().getConfigKey(),
        TestHelper.TEST_AAD_GROUP_NAME);

    validator.checkImplicitMsiGroupName(new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkImplicitMsiAadGroupInvalidGroupExpectError() throws Exception {
    Assume.assumeTrue(TestHelper.runImplicitMsiLiveTests());

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_IMPLICIT_MSI.unwrap()
        .getConfigKey(), "Yes");
    // use a randomly generated group name that should not exist
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMPLICIT_MSI_AAD_GROUP_NAME
            .unwrap().getConfigKey(),
        UUID.randomUUID().toString());

    validator.checkImplicitMsiGroupName(new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }
}
