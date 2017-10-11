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

package com.cloudera.director.azure.utils;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigException;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigRenderOptions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/**
 * AzurePluginConfigHelper tests.
 */
public class AzurePluginConfigHelperTest {
  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Verifies that the default Azure Plugin Config can be read in and isn't null.
   *
   * @throws Exception
   */
  @Test
  public void mergeConfigWithOnlyDefaultAzurePluginConfigNothingToMergeExpectParsesCorrectly()
      throws Exception {
    Config config = AzurePluginConfigHelper.mergeConfig(Configurations.AZURE_CONFIG_FILENAME, null);

    Assert.assertNotNull(config);
  }

  /**
   * Verifies that the default Azure Images Config can be read in and isn't null.
   *
   * @throws Exception
   */
  @Test
  public void mergeConfigWithOnlyDefaultImagesConfigNothingToMergeExpectParsesCorrectly()
      throws Exception {
    Config config = AzurePluginConfigHelper
        .mergeConfig(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE, null);

    Assert.assertNotNull(config);
  }

  /**
   * Makes a fake `azure-plugin.conf` to merge with the default and verifies that they are merged
   * correctly.
   *
   * N.B.: nested arrays like `supported-regions` are overwritten, not merged, as we merge at the
   * base level and don't consider that the children might be mergeable.
   *
   * @throws Exception
   */
  @Test
  public void mergeConfigWithUserDefinedAzurePluginConfigExpectParsesCorrectly() throws Exception {
    File configurationDirectory = temporaryFolder.getRoot();

    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));

    printWriter.println("provider {");
    printWriter.println("  supported-regions: [");
    printWriter.println("    \"fakeregion\"");
    printWriter.println("  ]");
    printWriter.println("}");
    printWriter.println("instance {");
    printWriter.println("  supported-instances: [");
    printWriter.println("    \"STANDARD_DS4\"");
    printWriter.println("  ],");
    printWriter.println("  supported-storage-account-types: [");
    printWriter.println("    \"" + SkuName.PREMIUM_LRS.toString() + "\"");
    printWriter.println("    \"" + SkuName.STANDARD_LRS.toString() + "\"");
    printWriter.println("    \"" + SkuName.STANDARD_RAGRS.toString() + "\"");
    printWriter.println("  ]");
    printWriter.println("}");
    printWriter.close();

    Config azurePluginConfig = AzurePluginConfigHelper
        .mergeConfig(Configurations.AZURE_CONFIG_FILENAME, configurationDirectory);

    try {
      // Verify that the config was correctly merged.
      Assert.assertEquals("STANDARD_DS4", azurePluginConfig.getConfig("instance")
          .getList("supported-instances").unwrapped().get(0));
      Assert.assertTrue(azurePluginConfig.getConfig("instance")
          .getList("supported-storage-account-types").unwrapped()
          .contains(SkuName.PREMIUM_LRS.toString()));
      Assert.assertTrue(azurePluginConfig.getConfig("instance")
          .getList("supported-storage-account-types").unwrapped()
          .contains(SkuName.STANDARD_LRS.toString()));
      Assert.assertTrue(azurePluginConfig.getConfig("instance")
          .getList("supported-storage-account-types").unwrapped()
          .contains(SkuName.STANDARD_RAGRS.toString()));
      Assert.assertEquals("^(([a-z])|([a-z][a-z0-9])|([a-z][a-z0-9-]{1,15}[a-z0-9]))$",
          azurePluginConfig.getConfig("instance").getString("instance-prefix-regex"));
      Assert.assertEquals("^(?=.{1,37}$)(([a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])\\.)*([a-z0-9]|[a-z" +
              "0-9][a-z0-9-]*[a-z0-9])$",
          azurePluginConfig.getConfig("instance").getString("dns-fqdn-suffix-regex"));
      Assert.assertEquals("fakeregion", azurePluginConfig.getConfig("provider")
          .getList("supported-regions").unwrapped().get(0));
    } catch (ConfigException e) {
      Assert.fail("azure-plugin.conf not merged correctly");
    }
  }

  /**
   * Makes a fake `images.conf` to merge with the default and verifies that they are merged
   * correctly.
   *
   * @throws Exception
   */
  @Test
  public void mergeConfigWithUserDefinedImagesConfigExpectParsesCorrectly() throws Exception {
    // Make a fake config to merge with the default
    File configurationDirectory = temporaryFolder.getRoot();
    File configFile = new File(configurationDirectory,
        Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));

    final String customName = "this-is-a-test-image";
    final String customPublisher = "test-publisher";
    final String customOffer = "test-offer";
    final String customSku = "TEST-SKU";
    final String customVersion = "latest";

    printWriter.println(customName + " {");
    printWriter.println("  publisher: " + customPublisher);
    printWriter.println("  offer: " + customOffer);
    printWriter.println("  sku: " + customSku);
    printWriter.println("  version: " + customVersion);
    printWriter.println("}");
    printWriter.close();

    Config configurableImages = AzurePluginConfigHelper
        .mergeConfig(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE, configurationDirectory);

    try {
      // Verify that the custom image was correctly brought in.
      Assert.assertEquals(customPublisher,
          configurableImages.getConfig(customName).getString("publisher"));
      Assert.assertEquals(customOffer,
          configurableImages.getConfig(customName).getString("offer"));
      Assert.assertEquals(customSku,
          configurableImages.getConfig(customName).getString("sku"));
      Assert.assertEquals(customVersion,
          configurableImages.getConfig(customName).getString("version"));

      // Verify that the cloudera/centos portion remains.
      Assert.assertEquals("cloudera",
          configurableImages.getConfig("cloudera-centos-67-latest").getString("publisher"));
      Assert.assertEquals("cloudera-centos-os",
          configurableImages.getConfig("cloudera-centos-67-latest").getString("offer"));
      Assert.assertEquals("6_7",
          configurableImages.getConfig("cloudera-centos-67-latest").getString("sku"));
      Assert.assertEquals("latest",
          configurableImages.getConfig("cloudera-centos-67-latest").getString("version"));
    } catch (ConfigException e) {
      Assert.fail("images.conf config not merged correctly");
    }
  }

  @Test
  public void parseConfigFromClasspathWithNonexistentPathExpectExceptionThrown() throws Exception {
    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.parseConfigFromClasspath("fake-classpath");
  }

  @Test
  public void parseConfigFromFileWithExistentFileWithDefaultConfigExpectNoExceptionThrown()
      throws Exception {
    File configurationDirectory = temporaryFolder.getRoot();

    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(configFile), "UTF-8")));

    printWriter.print(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .root()
        .render(ConfigRenderOptions.concise()));
    printWriter.close();

    AzurePluginConfigHelper.parseConfigFromFile(configFile);
  }

  @Test
  public void parseConfigFromFileWithNonexistentFileExpectExceptionThrown() throws Exception {
    File configurationDirectory = temporaryFolder.getRoot();
    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.parseConfigFromFile(configFile);
  }

  @Test
  public void validatePluginConfigWithDefaultConfigExpectNoExceptionThrown() throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);

    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithEmptyConfigExpectExceptionThrown() throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_PROVIDER);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithProviderMissingExpectExceptionThrown() throws Exception {
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.remove(Configurations.AZURE_CONFIG_PROVIDER);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_PROVIDER);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithInstanceMissingExpectExceptionThrown() throws Exception {
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.remove(Configurations.AZURE_CONFIG_INSTANCE);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithRequiredProviderSectionsMissingExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    ((Map<String, Object>) map.get(Configurations.AZURE_CONFIG_PROVIDER)).clear();

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_PROVIDER);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithRequiredInstanceSectionsMissingExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    ((Map<String, Object>) map.get(Configurations.AZURE_CONFIG_INSTANCE)).clear();

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validatePluginConfigWithRequiredSectionsMissingExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    ((Map<String, Object>) map.get(Configurations.AZURE_CONFIG_PROVIDER)).clear();
    ((Map<String, Object>) map.get(Configurations.AZURE_CONFIG_INSTANCE)).clear();

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_PROVIDER);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE);
    AzurePluginConfigHelper.validatePluginConfig(config);
  }

  @Test
  public void validateSupportedRegionsWithDefaultConfigExpectNoExceptionThrown() throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_PROVIDER);

    AzurePluginConfigHelper.validateSupportedRegions(config);
  }

  @Test
  public void validateSupportedRegionsWithMissingFieldExpectExceptionThrown() throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateSupportedRegions(config);
  }

  @Test
  public void validateSupportedRegionsWithNotListExpectExceptionThrown() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_CONFIG_PROVIDER_REGIONS, null);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateSupportedRegions(config);
  }

  @Test
  public void validateSupportedRegionsWithEmptyListExpectExceptionThrown() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_CONFIG_PROVIDER_REGIONS, new ArrayList<String>());

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateSupportedRegions(config);
  }

  @Test
  public void validateAzureBackendOperationPollingTimeoutWithDefaultConfigExpectNoExceptionThrown()
      throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_PROVIDER);

    AzurePluginConfigHelper.validateAzureBackendOperationPollingTimeout(config);
  }

  @Test
  public void validateAzureBackendOperationPollingTimeoutWithMissingFieldExpectExceptionThrown()
      throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureBackendOperationPollingTimeout(config);
  }

  @Test
  public void validateAzureBackendOperationPollingTimeoutWithNotIntExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS, null);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureBackendOperationPollingTimeout(config);
  }

  @Test
  public void validateAzureBackendOperationPollingTimeoutWithLessThanZeroExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS, -1);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateAzureBackendOperationPollingTimeout(config);
  }

  @Test
  public void validateAzureBackendOperationPollingTimeoutWithGreaterThanMaxExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS,
        Configurations.MAX_TASKS_POLLING_TIMEOUT_SECONDS + 1);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateAzureBackendOperationPollingTimeout(config);
  }

  @Test
  public void validateSupportedInstancesWithDefaultConfigExpectNoExceptionThrown() throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_INSTANCE);

    AzurePluginConfigHelper.validateSupportedInstances(config);
  }

  @Test
  public void validateSupportedInstancesWithMissingFieldExpectExceptionThrown() throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED);
    AzurePluginConfigHelper.validateSupportedInstances(config);
  }

  @Test
  public void validateSupportedInstancesWithNotListExpectExceptionThrown() throws Exception {
    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED, null);

    thrown.expect(ConfigException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED);
    AzurePluginConfigHelper.validateSupportedInstances(config);
  }

  @Test
  public void validateSupportedInstancesWithEmptyListExpectExceptionThrown() throws Exception {
    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED,
        new ArrayList<String>());

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateSupportedInstances(config);
  }

  @Test
  public void validateStorageAccountTypeWithDefaultExpectNoExceptionThrown() throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_INSTANCE);

    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithDeprecatedTypesNoExceptionThrown() throws Exception {
    ArrayList<String> storageAccountTypes =
        new ArrayList<>(Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet());

    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        storageAccountTypes);

    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithValidNonDefaultExpectNoExceptionThrown()
      throws Exception {

    ArrayList<String> storageAccountTypes = new ArrayList<>();
    storageAccountTypes.add(SkuName.STANDARD_LRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_GRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_RAGRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_ZRS.toString());
    storageAccountTypes.add(SkuName.PREMIUM_LRS.toString());

    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        storageAccountTypes);

    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithAbsentExpectExceptionThrown() throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES);
    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithNotListExpectExceptionThrown() throws Exception {
    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        null);

    thrown.expect(ConfigException.class);
    thrown.expectMessage(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES);
    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithEmptyListExpectExceptionThrown() throws Exception {
    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        new ArrayList<String>());

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void validateStorageAccountTypeWithInvalidListExpectExceptionThrown() throws Exception {
    final String fakeAccountType = "fake-storage-account-type";
    ArrayList<String> storageAccountTypes = new ArrayList<>();
    storageAccountTypes.add(SkuName.STANDARD_LRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_GRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_RAGRS.toString());
    storageAccountTypes.add(SkuName.STANDARD_ZRS.toString());
    storageAccountTypes.add(SkuName.PREMIUM_LRS.toString());
    storageAccountTypes.add(fakeAccountType);

    Config config = buildConfigWith(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        storageAccountTypes);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(fakeAccountType);
    AzurePluginConfigHelper.validateStorageAccountType(config);
  }

  @Test
  public void setAzurePluginConfigWithSetOnceExpectConfigSet() throws Exception {
    // must set this back to null using reflection
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);
    AzurePluginConfigHelper.setAzurePluginConfig(config);

    Assert.assertNotNull(AzurePluginConfigHelper.getAzurePluginConfig());
  }

  @Test
  public void setAzurePluginConfigWithSetTwiceExpectConfigDoesNotChange() throws Exception {
    // must set this back to null using reflection
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);
    AzurePluginConfigHelper.setAzurePluginConfig(config);

    Map<String, Object> map = new HashMap<>();
    Config emptyConfig = ConfigFactory.parseMap(map);
    AzurePluginConfigHelper.setAzurePluginConfig(emptyConfig);

    Assert
        .assertFalse(map.equals(AzurePluginConfigHelper.getAzurePluginConfig().root().unwrapped()));
    Assert.assertFalse(AzurePluginConfigHelper.getAzurePluginConfig().root().unwrapped().isEmpty());
  }

  @Test
  public void setConfigurableImagesWithSetOnceExpectConfigSet() throws Exception {
    // must set this back to null using reflection
    TestHelper.setConfigurableImagesNull();
    Assert.assertNull(AzurePluginConfigHelper.getConfigurableImages());

    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    AzurePluginConfigHelper.setConfigurableImages(config);

    Assert.assertNotNull(AzurePluginConfigHelper.getConfigurableImages());
  }

  @Test
  public void setConfigurableImagesWithSetTwiceExpectConfigDoesNotChange() throws Exception {
    // must set this back to null using reflection
    TestHelper.setConfigurableImagesNull();
    Assert.assertNull(AzurePluginConfigHelper.getConfigurableImages());

    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    AzurePluginConfigHelper.setConfigurableImages(config);

    Map<String, Object> map = new HashMap<>();
    Config emptyConfig = ConfigFactory.parseMap(map);
    AzurePluginConfigHelper.setConfigurableImages(emptyConfig);

    Assert.assertFalse(
        map.equals(AzurePluginConfigHelper.getConfigurableImages().root().unwrapped()));
    Assert
        .assertFalse(AzurePluginConfigHelper.getConfigurableImages().root().unwrapped().isEmpty());
  }

  @Test
  public void validateResourcesWithDefaultsReturnsTrue() throws Exception {
    // by default it's true
    Assert.assertTrue(AzurePluginConfigHelper.validateResources());
  }

  @Test
  public void validateResourcesWithFlagMissingReturnsTrue() throws Exception {
    // set validate resources to false
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.remove(Configurations.AZURE_VALIDATE_RESOURCES);

    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Assert.assertTrue(AzurePluginConfigHelper.validateResources());
  }

  @Test
  public void validateResourcesWithFlagFalseReturnsFalse() throws Exception {
    // set validate resources to false
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.put(Configurations.AZURE_VALIDATE_RESOURCES, false);

    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Assert.assertFalse(AzurePluginConfigHelper.validateResources());
  }

  @Test
  public void validateCredentialsWithDefaultsReturnsTrue() throws Exception {
    // by default it's true
    Assert.assertTrue(AzurePluginConfigHelper.validateCredentials());
  }

  @Test
  public void validateCredentialsWithFlagMissingReturnsTrue() throws Exception {
    // set validate resources to false
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.remove(Configurations.AZURE_VALIDATE_CREDENTIALS);

    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Assert.assertTrue(AzurePluginConfigHelper.validateCredentials());
  }

  @Test
  public void validateCredentialsWithFlagFalseReturnsFalse() throws Exception {
    // set validate resources to false
    TestHelper.setAzurePluginConfigNull();
    Assert.assertNull(AzurePluginConfigHelper.getAzurePluginConfig());

    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.put(Configurations.AZURE_VALIDATE_CREDENTIALS, false);

    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Assert.assertFalse(AzurePluginConfigHelper.validateCredentials());
  }

  @Test
  public void validateAzureSdkConnectionTimeoutWithDefaultConfigExpectNoExceptionThrown()
      throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_PROVIDER);

    AzurePluginConfigHelper.validateAzureSdkConnTimeout(config);
  }

  @Test
  public void validateAzureSdkConnectionTimeoutWithDefaultConfigWithLessThanZeroExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS, -1);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateAzureSdkConnTimeout(config);
  }

  @Test
  public void validateAzureSdkConnectionTimeoutWithMissingFieldExpectExceptionThrown()
      throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkConnTimeout(config);
  }

  @Test
  public void validateAzureSdkConnectionTimeoutWithNotIntExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS, "foobar");

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkConnTimeout(config);
  }

  @Test
  public void validateAzureSdkReadTimeoutWithDefaultConfigExpectNoExceptionThrown()
      throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_PROVIDER);

    AzurePluginConfigHelper.validateAzureSdkReadTimeout(config);
  }

  @Test
  public void validateAzureSdkReadTimeoutWithDefaultConfigWithLessThanZeroExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS, -1);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateAzureSdkReadTimeout(config);
  }

  @Test
  public void validateAzureSdkReadTimeoutWithMissingFieldExpectExceptionThrown()
      throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkReadTimeout(config);
  }

  @Test
  public void validateAzureSdkReadTimeoutWithNotIntExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS, "foobar");

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkReadTimeout(config);
  }

  @Test
  public void validateAzureSdkMaxIdleConnWithDefaultConfigExpectNoExceptionThrown()
      throws Exception {
    Config config = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME)
        .getConfig(Configurations.AZURE_CONFIG_PROVIDER);

    AzurePluginConfigHelper.validateAzureSdkMaxIdleConn(config);
  }

  @Test
  public void validateAzureSdkMaxIdleConnWithDefaultConfigWithLessThanZeroExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_MAX_IDLE_CONN, -1);

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(IllegalArgumentException.class);
    AzurePluginConfigHelper.validateAzureSdkMaxIdleConn(config);
  }

  @Test
  public void validateAzureSdkMaxIdleConnWithMissingFieldExpectExceptionThrown()
      throws Exception {
    Config config = ConfigFactory.parseMap(new HashMap<String, Object>());

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkMaxIdleConn(config);
  }

  @Test
  public void validateAzureSdkMaxIdleConnWithNotIntExpectExceptionThrown()
      throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put(Configurations.AZURE_SDK_CONFIG_MAX_IDLE_CONN, "foobar");

    Config config = ConfigFactory.parseMap(map);

    thrown.expect(ConfigException.class);
    AzurePluginConfigHelper.validateAzureSdkMaxIdleConn(config);
  }

  /**
   * Helper method to build Config objects.
   *
   * @param name field name to be in the config object
   * @param value object to put in the config object
   * @return the built config
   */
  private Config buildConfigWith(String name, Object value) {
    Map<String, Object> map = new HashMap<>();
    map.put(name, value);

    return ConfigFactory.parseMap(map);
  }
}
