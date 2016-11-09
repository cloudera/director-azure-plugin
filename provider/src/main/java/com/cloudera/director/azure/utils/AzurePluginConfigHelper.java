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

import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES;

import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.microsoft.azure.management.storage.models.AccountType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class provides a set of methods to read & validate Azure Director Plugin configs.
 */
public class AzurePluginConfigHelper {
  /**
   * Helper to parse the specified configuration file from the classpath.
   *
   * @param configPath the path to the configuration file
   * @return the parsed configuration
   */
  public static Config parseConfigFromClasspath(String configPath) throws IOException {
    ConfigParseOptions options = ConfigParseOptions.defaults()
      .setSyntax(ConfigSyntax.CONF)
      .setAllowMissing(false);
    return ConfigFactory.parseResourcesAnySyntax(AzureLauncher.class, configPath, options);
  }

  /**
   * Helper to parses the specified configuration file.
   *
   * @param configFile the configuration file
   * @return the parsed configuration
   */
  public static Config parseConfigFromFile(File configFile) throws IOException {
    ConfigParseOptions options = ConfigParseOptions.defaults()
      .setSyntax(ConfigSyntax.CONF)
      .setAllowMissing(false);
    return ConfigFactory.parseFileAnySyntax(configFile, options);
  }

  /**
   * Helper to validate plugin configs read from classpath or director config directory.
   *
   * @param cfg parsed plugin config
   * @throws RuntimeException exception is thrown if the parsed config is missing necessary parts
   */
  public static void validatePluginConfig(Config cfg) throws RuntimeException {
    Config providerSection = cfg.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
    ConfigList supportedRegions = providerSection.getList(
      Configurations.AZURE_CONFIG_PROVIDER_REGIONS);
    if (supportedRegions.size() == 0) {
      throw new RuntimeException(
        "Supported regions in the the Azure director plugin configuration is empty.");
    }
    Config instanceSection = cfg.getConfig(Configurations.AZURE_CONFIG_INSTANCE);
    ConfigList instanceTypes = instanceSection.getList(
      Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED);
    if (instanceTypes.size() == 0) {
      throw new RuntimeException(
        "Supported instance types in the the Azure director plugin configuration is empty.");
    }

    validateStorageAccountType(instanceSection.getStringList(
      AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES));
  }

  /**
   * Helper to validate the Storage Account Type portion of the plugin config.
   *
   * @param storageAccountTypes list of storage account type strings to be checked
   * @throws IllegalArgumentException if a storage account type in the list is not valid
   */
  static void validateStorageAccountType(List<String> storageAccountTypes) {
    for (String storageAccountType : storageAccountTypes) {
      try {
        AccountType.valueOf(storageAccountType);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Storage Account Type '%s' is not a valid Azure Storage Account Type. Valid types: '%s'",
          storageAccountType, Arrays.asList(AccountType.values())));
      }
    }
  }

  /**
   * Helper to read and parse two config files and merge them together as follows:
   * 1. Read and parse the `filename` file
   * 2. Read and parse the `filename` file in the `configurationDirectory` directory
   * 3. Merge the two, with values in `configurationDirectory` + `filename` overriding values in
   * `filename`
   *
   * @param filename               file name of the config file to be parsed
   * @param configurationDirectory directory in which the config file lives
   * @return                       merged config
   */
  public static Config mergeConfig(String filename, File configurationDirectory) {
    Config config = null;

    // Read in the default config file
    try {
      config = AzurePluginConfigHelper.parseConfigFromClasspath(filename);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Check if an additional config file exists in the configuration directory.
    File configFile = new File(configurationDirectory, filename);
    if (configFile.canRead()) {
      try {
        Config configFromFile = AzurePluginConfigHelper.parseConfigFromFile(configFile);

        // Merge the two configurations, with values in `configFromFile` overriding values in
        // `config`.
        config = configFromFile.withFallback(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return config;
  }
}
