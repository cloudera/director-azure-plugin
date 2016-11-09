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
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.models.AccountType;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigException;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigValue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for AzurePluginConfigHelper class
 */
public class AzurePluginConfigHelperTest {
  private static final Logger LOG = LoggerFactory.getLogger(AzurePluginConfigHelperTest.class);

  @Rule
  public TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testBasicCase() throws Exception {
    // try to read and parse plugin config file
    Config cfg = AzurePluginConfigHelper.parseConfigFromClasspath(
      Configurations.AZURE_CONFIG_FILENAME);
    // verify the plugin config file
    AzurePluginConfigHelper.validatePluginConfig(cfg);
    // print the content of the config file in classpath
    for (Map.Entry<String, ConfigValue> e : cfg.entrySet()) {
      LOG.info(e.getKey() + "\t" + e.getValue());
    }
  }

  @Test
  public void testBasicErrorCase() throws Exception {
    // wrong file name
    try {
      AzurePluginConfigHelper.parseConfigFromClasspath("foobar");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("resource not found on classpath"));
    }
    // empty config
    try {
      Config cfg = ConfigFactory.empty();
      AzurePluginConfigHelper.validatePluginConfig(cfg);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("No configuration setting found"));
    }
  }

  //
  // Check that the Storage Account Types are all valid Azure AccountType(s)
  //

  @Test
  public void validatePluginConfig_defaultStorageAccountTypes_success() throws Exception {
    final List<String> storageAccountTypes = new ArrayList<String>() {{
      add(AccountType.StandardLRS.toString());
      add(AccountType.PremiumLRS.toString());
    }};

    try {
      AzurePluginConfigHelper.validateStorageAccountType(storageAccountTypes);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void validatePluginConfig_validNonDefaultStorageAccountTypes_success() throws Exception {
    final List<String> storageAccountTypes = new ArrayList<String>() {{
      add(AccountType.StandardLRS.toString());
      add(AccountType.StandardZRS.toString());
      add(AccountType.StandardGRS.toString());
      add(AccountType.StandardRAGRS.toString());
      add(AccountType.PremiumLRS.toString());
    }};

    try {
      AzurePluginConfigHelper.validateStorageAccountType(storageAccountTypes);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void validatePluginConfig_invalidStorageAccountTypes_error() throws Exception {
    final String fakeAccountType = "fake-account-type";
    final List<String> storageAccountTypes = new ArrayList<String>() {{
      add(AccountType.StandardLRS.toString());
      add(AccountType.StandardZRS.toString());
      add(AccountType.StandardGRS.toString());
      add(AccountType.StandardRAGRS.toString());
      add(AccountType.PremiumLRS.toString());
      add(fakeAccountType);
    }};

    try {
      AzurePluginConfigHelper.validateStorageAccountType(storageAccountTypes);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(fakeAccountType));
    }
  }

  /**
   * Tests that the default configs can be read in and aren't null.
   *
   * @throws Exception
   */
  @Test
  public void mergeConfig_onlyDefaultConfigNothingToMerge_parsesCorrectly() throws Exception {
    Config config = null;
    File configurationDirectory = null;

    config = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIG_FILENAME,
      configurationDirectory
    );
    assertNotNull(config);

    config = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIGURABLE_IMAGES_FILE,
      configurationDirectory
    );
    assertNotNull(config);
  }

  /**
   * Makes a fake `azure-plugin.conf` to merge with the default and verifies that they are merged
   * correctly.
   * <p>
   * N.B.: nested arrays like `supported-regions` are overwritten, not merged, as we merge at the
   * base level and don't consider that the children might be mergeable.
   *
   * @throws Exception
   */
  @Test
  public void mergeAzureConfig_withUserDefinedConfig_parsesCorrectly() throws Exception {
    File configurationDirectory = TEMPORARY_FOLDER.getRoot();

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
    printWriter.println("    \"" + AccountType.PremiumLRS.toString() + "\"");
    printWriter.println("    \"" + AccountType.StandardLRS.toString() + "\"");
    printWriter.println("    \"" + AccountType.StandardRAGRS.toString() + "\"");
    printWriter.println("  ]");
    printWriter.println("}");
    printWriter.close();

    Config azurePluginConfig = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIG_FILENAME,
      configurationDirectory
    );

    try {
      // Verify that the config was correctly merged.
      assertEquals("STANDARD_DS4", azurePluginConfig.getConfig("instance")
        .getList("supported-instances").unwrapped().get(0));
      assertTrue(azurePluginConfig.getConfig("instance").getList("supported-storage-account-types")
        .unwrapped().contains(AccountType.PremiumLRS.toString()));
      assertTrue(azurePluginConfig.getConfig("instance").getList("supported-storage-account-types")
        .unwrapped().contains(AccountType.StandardLRS.toString()));
      assertTrue(azurePluginConfig.getConfig("instance").getList("supported-storage-account-types")
        .unwrapped().contains(AccountType.StandardRAGRS.toString()));
      assertEquals("^[a-z][a-z0-9-]{1,15}[a-z0-9]$", azurePluginConfig.getConfig("instance")
        .getString("instance-prefix-regex"));
      assertEquals(
        "^(?=.{1,37}$)(([a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])\\.)*([a-z0-9]|[a-z0-9][a-z0-9-]*[a-z0-9])$",
        azurePluginConfig.getConfig("instance").getString("dns-fqdn-suffix-regex"));
      assertEquals("fakeregion", azurePluginConfig.getConfig("provider")
        .getList("supported-regions").unwrapped().get(0));
    } catch (ConfigException e) {
      fail("azure-plugin.conf not merged correctly");
    }
  }

  /**
   * Makes a fake `images.conf` to merge with the default and verifies that they are merged
   * correctly.
   *
   * @throws Exception
   */
  @Test
  public void mergeImagesConfig_withUserDefinedConfig_parsesCorrectly() throws Exception {
    // Make a fake config to merge with the default
    File configurationDirectory = TEMPORARY_FOLDER.getRoot();
    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(configFile), "UTF-8")));
    printWriter.println("cloudera-rhel-6-latest {");
    printWriter.println("  publisher: rhel");
    printWriter.println("  offer: cloudera-rhel-6");
    printWriter.println("  sku: CLOUDERA-RHEL-6");
    printWriter.println("  version: latest");
    printWriter.println("}");
    printWriter.close();

    Config configurableImages = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIGURABLE_IMAGES_FILE,
      configurationDirectory
    );

    try {
      // Verify that the rhel portion was correctly brought in.
      assertEquals("cloudera-rhel-6", configurableImages.getConfig("cloudera-rhel-6-latest").getString("offer"));
      assertEquals("rhel", configurableImages.getConfig("cloudera-rhel-6-latest").getString("publisher"));
      assertEquals("CLOUDERA-RHEL-6", configurableImages.getConfig("cloudera-rhel-6-latest").getString("sku"));
      assertEquals("latest", configurableImages.getConfig("cloudera-rhel-6-latest").getString("version"));
      assertEquals("cloudera-rhel-6", configurableImages.getConfig("cloudera-rhel-6-latest").getString("offer"));

      // Verify that the cloudera/centos portion remains.
      assertEquals("cloudera-centos-6", configurableImages.getConfig("cloudera-centos-6-latest").getString("offer"));
      assertEquals("cloudera", configurableImages.getConfig("cloudera-centos-6-latest").getString("publisher"));
      assertEquals("CLOUDERA-CENTOS-6", configurableImages.getConfig("cloudera-centos-6-latest").getString("sku"));
      assertEquals("latest", configurableImages.getConfig("cloudera-centos-6-latest").getString("version"));
      assertEquals("cloudera-centos-6", configurableImages.getConfig("cloudera-centos-6-latest").getString("offer"));
    } catch (ConfigException e) {
      fail("images.conf config not merged correctly");
    }
  }

  // FIXME add a test case to verify specific sections missing. Manually testing for now.
}
