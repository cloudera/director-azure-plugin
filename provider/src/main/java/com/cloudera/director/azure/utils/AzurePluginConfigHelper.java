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

import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.microsoft.azure.management.storage.SkuName;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a set of methods to read and validate Azure Director Plugin configs.
 */
public class AzurePluginConfigHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AzurePluginConfigHelper.class);

  // Static config objects
  // azure-plugin.conf
  private static Config azurePluginConfig = null;
  // images.conf
  private static Config configurableImages = null;

  /**
   * Helper to read and parse two config files and merge them together as follows:
   * 1. Read and parse the `filename` file
   * 2. Read and parse the `filename` file in the `configurationDirectory` directory
   * 3. Merge the two, with values in `configurationDirectory` + `filename` overriding values in
   * `filename`
   *
   * @param filename file name of the config file to be parsed
   * @param configurationDirectory directory in which the config file lives
   * @return merged config
   */
  public static Config mergeConfig(String filename, File configurationDirectory) {
    Config config = null;

    // Read in the required default config file.
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

  /**
   * Validates the azure plugin config:
   * - all required fields are present and non-null
   * - individual checks for some fields
   *
   * @param config parsed azure plugin config
   * @throws RuntimeException exception is thrown if the parsed config is invalid or missing
   * necessary parts
   */
  public static void validatePluginConfig(Config config) throws RuntimeException {
    // Holds all errors.
    List<String> errors = new ArrayList<>();

    // Provider section checks
    try {
      Config providerSection = config.getConfig(Configurations.AZURE_CONFIG_PROVIDER);

      // Ensure all provider values are present and non-null at the given path.
      for (String param : Configurations.getRequiredProviderFields()) {
        if (!providerSection.hasPath(param)) {
          errors.add("Required field: " + Configurations.AZURE_CONFIG_PROVIDER + "." + param +
              " is missing or empty.");
        }
      }

      try {
        validateSupportedRegions(providerSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }

      try {
        validateAzureBackendOperationPollingTimeout(providerSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }

      try {
        validateAzureSdkConnTimeout(providerSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }

      try {
        validateAzureSdkReadTimeout(providerSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }

      try {
        validateAzureSdkMaxIdleConn(providerSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }
    } catch (ConfigException e) {
      errors.add(e.getMessage());
    }

    // Instance section checks
    try {
      Config instanceSection = config.getConfig(Configurations.AZURE_CONFIG_INSTANCE);

      // Ensure instance values are present and non-null at the given path.
      for (String param : Configurations.getRequiredInstanceFields()) {
        if (!instanceSection.hasPath(param)) {
          errors.add("Required path: " + Configurations.AZURE_CONFIG_INSTANCE + "." + param +
              " is missing or empty.");
        }
      }

      try {
        validateStorageAccountType(instanceSection);
      } catch (ConfigException | IllegalArgumentException e) {
        errors.add(e.getMessage());
      }
    } catch (ConfigException e) {
      errors.add(e.getMessage());
    }

    // Log then throw the errors.
    if (errors.size() > 0) {
      // Accumulate all the errors together and trim any trailing whitespace.
      StringBuilder sb = new StringBuilder();
      for (String error : errors) {
        sb.append(error).append(" ");
      }
      String errorMessage = sb.toString().trim();

      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }
  }

  /**
   * Sets the static plugin config. The plugin can only be set once, and will remain the same until
   * Director is restarted.
   */
  public synchronized static void setAzurePluginConfig(Config config) {
    if (azurePluginConfig == null) {
      LOG.info("Azure Plugin Config initializing to: {}.", config);
      azurePluginConfig = config;
    } else {
      LOG.warn("Azure Plugin Config was already initialized - ignoring the new config of: {}.",
          config.root().unwrapped());
    }
  }

  public synchronized static Config getAzurePluginConfig() {
    return azurePluginConfig;
  }

  /**
   * Sets the images config. The images can only be set once, and will remain the same until
   * Director is restarted.
   */
  public synchronized static void setConfigurableImages(Config config) {
    if (configurableImages == null) {
      LOG.info("Configurable Images Config initializing to: {}.", config);
      configurableImages = config;
    } else {
      LOG.warn("Configurable Images Config was already initialized - ignoring the new config of: " +
          "{}.", config.root().unwrapped());
    }
  }

  /**
   * Gets the currently set images config.
   *
   * @return the images config, or null if not set.
   */
  public synchronized static Config getConfigurableImages() {
    return configurableImages;
  }

  /**
   * Gets the provider section of the current set config
   *
   * @return the provider section of the current config
   */
  public synchronized static Config getAzurePluginConfigProviderSection() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
  }

  /**
   * Gets the instance section of the current set config
   *
   * @return the instance section of the set config
   */
  public synchronized static Config getAzurePluginConfigInstanceSection() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_INSTANCE);
  }

  /**
   * Gets the config value for whether to validate Azure resources from the current set config.
   * Defaults to true if the field is missing or the wrong type.
   *
   * @return true if resources validator at provider and instance level checks should be enforced
   */
  public synchronized static boolean validateResources() {
    boolean flag;
    try {
      flag = azurePluginConfig.getBoolean(Configurations.AZURE_VALIDATE_RESOURCES);
    } catch (Exception e) {
      return true;
    }
    return flag;
  }

  /**
   * Gets the config value for whether to validate Azure credentials from the current set config.
   * Defaults to true if the field is missing or the wrong type.
   *
   * @return true if all credential checks should be enforced
   */
  public synchronized static boolean validateCredentials() {
    boolean flag;
    try {
      flag = azurePluginConfig.getBoolean(Configurations.AZURE_VALIDATE_CREDENTIALS);
    } catch (Exception e) {
      return true;
    }
    return flag;
  }

  /**
   * Validates that the passed in config object contains a valid supported regions section by
   * checking that the param is:
   * - not absent, null, or wrong type
   * - not an empty list
   *
   * @param providerSection the provider section of the Azure Plugin config
   * @throws IllegalArgumentException if the regions list is empty
   * @throws ConfigException if the regions config section is missing or the wrong type
   */
  static void validateSupportedRegions(Config providerSection) throws IllegalArgumentException,
      ConfigException {
    ConfigList supportedRegions =
        providerSection.getList(Configurations.AZURE_CONFIG_PROVIDER_REGIONS);

    if (supportedRegions.size() == 0) {
      throw new IllegalArgumentException(String.format("The list of supported regions \"%s\" in " +
              "the the Azure director plugin configuration is empty.",
          Configurations.AZURE_CONFIG_PROVIDER_REGIONS));
    }
  }

  /**
   * Validates that the passed in config object contains a valid polling timeout section by
   * checking that the param is:
   * - not absent, null, or wrong type
   * - between 0 and MAX_TASKS_POLLING_TIMEOUT_SECONDS (3600), exclusive
   *
   * @param providerSection the provider section of the Azure Plugin config
   * @throws IllegalArgumentException if the polling timeout is not within range
   * @throws ConfigException if the polling timeout config section is missing or the wrong type
   */
  static void validateAzureBackendOperationPollingTimeout(Config providerSection) throws
      IllegalArgumentException, ConfigException {
    int timeout = providerSection
        .getInt(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS);
    int maxTimeoutValue = Configurations.MAX_TASKS_POLLING_TIMEOUT_SECONDS;

    if (timeout < 0 || timeout > maxTimeoutValue) {
      throw new IllegalArgumentException(String.format("Azure Plugin Config field \"%s\" must " +
              "have a value in seconds that is greater than 0 and less than %d",
          Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS,
          maxTimeoutValue));
    }
  }

  /**
   * Validates that the passed in config object contains a valid storage account types section by
   * checking that the param is:
   * - not absent, null, or wrong type
   * - not an empty list
   * - contains only valid Storage Accounts
   *
   * @param instanceSection the instance section of the Azure Plugin config
   * @throws IllegalArgumentException if the list is empty or a storage account type in the list is
   * not valid
   * @throws ConfigException if the storage account type list config section is missing or the wrong
   * type
   */
  static void validateStorageAccountType(Config instanceSection) throws IllegalArgumentException,
      ConfigException {
    List<String> storageAccountTypes = instanceSection
        .getStringList(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES);

    if (storageAccountTypes.size() == 0) {
      throw new IllegalArgumentException(String.format(
          "The list of Storage Accounts to validate is empty. Valid types: %s.",
          Arrays.asList(SkuName.values())));
    }

    for (String storageAccountType : storageAccountTypes) {
      storageAccountType = Configurations.convertStorageAccountTypeString(storageAccountType);

      if (SkuName.fromString(storageAccountType) == null) {
        throw new IllegalArgumentException(String.format("Storage Account Type '%s' is not " +
                "valid. Valid types: %s, valid deprecated types: %s.",
            storageAccountType,
            Arrays.asList(SkuName.values()),
            Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()));
      }
    }
  }

  /**
   * Validates that the passed in config object contains a valid Azure SDK connection timeout value
   * by checking that the param is:
   * - not absent, null, or wrong type
   * - greater than 0
   *
   * @param providerSection the provider section of the Azure Plugin config
   * @throws IllegalArgumentException if the config value is less than zero
   * @throws ConfigException if the config section is missing or has the wrong type
   */
  static void validateAzureSdkConnTimeout(Config providerSection) throws
      IllegalArgumentException, ConfigException {
    int timeout = providerSection
        .getInt(Configurations.AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS);

    if (timeout < 0) {
      throw new IllegalArgumentException(String.format("Azure Plugin Config field \"%s\" must " +
              "have a value in seconds that is greater than 0.",
          Configurations.AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS));
    }
  }

  /**
   * Validates that the passed in config object contains a valid Azure SDK read timeout value by
   * checking that the param is:
   * - not absent, null, or wrong type
   * - greater than 0
   *
   * @param providerSection the provider section of the Azure Plugin config
   * @throws IllegalArgumentException if the config value is less than zero
   * @throws ConfigException if the config section is missing or has the wrong type
   */
  static void validateAzureSdkReadTimeout(Config providerSection) throws
      IllegalArgumentException, ConfigException {
    int timeout = providerSection
        .getInt(Configurations.AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS);

    if (timeout < 0) {
      throw new IllegalArgumentException(String.format("Azure Plugin Config field \"%s\" must " +
              "have a value in seconds that is greater than 0.",
          Configurations.AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS));
    }
  }

  /**
   * Validates that the passed in config object contains a valid Azure SDK max idle connection
   * value by checking that the param is:
   * - not absent, null, or wrong type
   * - greater than 0
   *
   * @param providerSection the provider section of the Azure Plugin config
   * @throws IllegalArgumentException if the config value is less than zero
   * @throws ConfigException if the config section is missing or has the wrong type
   */
  static void validateAzureSdkMaxIdleConn(Config providerSection) throws
      IllegalArgumentException, ConfigException {
    int timeout = providerSection
        .getInt(Configurations.AZURE_SDK_CONFIG_MAX_IDLE_CONN);

    if (timeout < 0) {
      throw new IllegalArgumentException(String.format("Azure Plugin Config field \"%s\" must " +
              "have a value that is greater than 0.",
          Configurations.AZURE_SDK_CONFIG_MAX_IDLE_CONN));
    }
  }

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
   * Helper to parse the specified configuration file.
   *
   * @param configFile the configuration file
   * @return the parsed configuration
   */
  static Config parseConfigFromFile(File configFile) throws IOException {
    ConfigParseOptions options = ConfigParseOptions.defaults()
        .setSyntax(ConfigSyntax.CONF)
        .setAllowMissing(false);
    return ConfigFactory.parseFileAnySyntax(configFile, options);
  }

  /**
   * Helper function to get Azure backend timeout value (in seconds) from plugin config.
   *
   * @return Azure backend timeout value (in seconds) from plugin config
   */
  public static synchronized int getAzureBackendOpPollingTimeOut() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER)
        .getInt(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS);
  }

  /**
   * Helper function to get Azure vmss timeout value (in seconds) from plugin config.
   *
   * @return Azure vmss timeout value (in seconds) from plugin config
   */
  public static synchronized long getVMSSOpTimeout() {
    Config providerConfig = azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
    long timeoutInSec = providerConfig.hasPath(Configurations.AZURE_CONFIG_PROVIDER_VMSS_OPERATION_TIMEOUT_SECONDS) ?
        providerConfig.getLong(Configurations.AZURE_CONFIG_PROVIDER_VMSS_OPERATION_TIMEOUT_SECONDS) :
        getAzureBackendOpPollingTimeOut();
    return timeoutInSec < 0 ? Long.MAX_VALUE : timeoutInSec;
  }

  /**
   * Helper function to get Azure SDK connection timeout value (in seconds) from plugin config.
   *
   * @return Azure SDK connection timeout value (in seconds)
   */
  public static synchronized int getAzureSdkConnectionTimeout() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER)
        .getInt(Configurations.AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS);
  }

  /**
   * Helper function to get Azure SDK read timeout value (in seconds) from plugin config.
   *
   * @return Azure SDK read timeout value (in seconds)
   */
  public static synchronized int getAzureSdkReadTimeout() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER)
        .getInt(Configurations.AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS);
  }

  /**
   * Helper function to get Azure SDK max idle connection(s) from plugin config.
   *
   * @return Azure SDK max idle connection(s)
   */
  public static synchronized int getAzureSdkMaxIdleConn() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER)
        .getInt(Configurations.AZURE_SDK_CONFIG_MAX_IDLE_CONN);
  }
}
