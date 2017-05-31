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
import com.microsoft.azure.management.storage.models.AccountType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class provides a set of methods to read & validate Azure Director Plugin configs.
 */
public class AzurePluginConfigHelper {
  private static final Logger LOG = LoggerFactory.getLogger(AzurePluginConfigHelper.class);

  // Static config objects
  // azure-plugin.conf
  private static Config azurePluginConfig = null;
  // images.conf
  private static Config configurableImages = null;

  // List of required values from the instance section of the configuration.
  private static String[] requiredInstanceParams = {
    Configurations.AZURE_CONFIG_INSTANCE_DNS_LABEL_REGEX,
    Configurations.AZURE_CONFIG_INSTANCE_FQDN_SUFFIX_REGEX,
    Configurations.AZURE_CONFIG_INSTANCE_MAXIMUM_STANDARD_DISK_SIZE,
    Configurations.AZURE_CONFIG_INSTANCE_PREMIUM_DISK_SIZES,
    Configurations.AZURE_CONFIG_DISALLOWED_USERNAMES
  };

  // List of required values from the provider section of the configuration.
  private static String[] requiredProviderParams = {
    Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS
  };

  /**
   * Sets the static plugin config. The plugin can only be set once, and will remain the same until
   * Director is restarted.
   */
  public synchronized static void setAzurePluginConfig(Config cfg) {
    if (azurePluginConfig == null) {
      LOG.info("Azure Plugin Config initializing to: {}.", cfg);
      azurePluginConfig = cfg;
    } else {
      LOG.warn("Azure Plugin Config was already initialized - ignoring the new config of: {}.",
        cfg);
    }
  }

  /**
   * This method is only used for testing
   */
  public synchronized static void mergeAzurePluginConfig(Config newConfig) {
    azurePluginConfig = newConfig.withFallback(azurePluginConfig);
    LOG.info("Azure Plugin Config updated to: {}.", azurePluginConfig);
  }

  public synchronized static Config getAzurePluginConfig() {
    return azurePluginConfig;
  }

  public synchronized static Config getAzurePluginConfigInstanceSection() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_INSTANCE);
  }

  public synchronized static Config getAzurePluginConfigProviderSection() {
    return azurePluginConfig.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
  }

  /**
   * Get the config value for whether to validate Azure resources
   *
   * @return True if resources validator at provider and instance level checks should be enforced
   */
  public synchronized static boolean getValidateResourcesFlag() {
    return azurePluginConfig.getBoolean(Configurations.AZURE_VALIDATE_RESOURCES);
  }

  /**
   * Get the config value for whether to validate Azure credentials
   *
   * @return True if all credential checks should be enforced
   */
  public synchronized static boolean getValidateCredentialsFlag() {
    return azurePluginConfig.getBoolean(Configurations.AZURE_VALIDATE_CREDENTIALS);
  }

  /**
   * Get the config value for whether to use static private IP address
   *
   * @return True (default) if all VMs should use statically assigned private IP addresses
   */
  public synchronized static boolean getUseStaticPrivateIp() {
    return azurePluginConfig.getBoolean(Configurations.AZURE_USE_STATIC_PRIVATE_IP);
  }

  /**
   * Sets the images config. The images can only be set once, and will remain the same until
   * Director is restarted.
   */
  public synchronized static void setConfigurableImages(Config cfg) {
    if (configurableImages == null) {
      LOG.info("Configurable Images Config initializing to: {}.", cfg);
      configurableImages = cfg;
    } else {
      LOG.warn("Configurable Images Config was already initialized - ignoring the new config of: " +
        "{}.", cfg);
    }
  }

  public synchronized static Config getConfigurableImages() {
    return configurableImages;
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

    StringBuffer errors = new StringBuffer();

    Config providerSection = cfg.getConfig(Configurations.AZURE_CONFIG_PROVIDER);
    try {
      ConfigList supportedRegions =
        providerSection.getList(Configurations.AZURE_CONFIG_PROVIDER_REGIONS);
      if (supportedRegions.size() == 0) {
      errors.append("The list of supported regions (" +
                    Configurations.AZURE_CONFIG_PROVIDER_REGIONS +
                    ") in the the Azure director plugin configuration is empty.");
      }
    } catch (ConfigException e) {
      errors.append("\n" + e.getMessage());
    }

    // Ensure provider values are present and non-null at the given path.
    for (String param : requiredProviderParams) {
      if (!providerSection.hasPath(param)) {
        errors.append("\nRequired path: " + param + " in the section " + providerSection +
                      " is missing or empty.");
      }
    }

    Config instanceSection = cfg.getConfig(Configurations.AZURE_CONFIG_INSTANCE);
    try {
      ConfigList instanceTypes =
        instanceSection.getList(Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED);
      if (instanceTypes.size() == 0) {
        errors.append("\nThe list of supported instance types (" +
                      Configurations.AZURE_CONFIG_INSTANCE_SUPPORTED +
                      ") in the the Azure director plugin configuration is empty.");
      }
    } catch (ConfigException e) {
      errors.append("\n" + e.getMessage());
    }

    // Ensure instance values are present and non-null at the given path.
    for (String param : requiredInstanceParams) {
      if (!instanceSection.hasPath(param)) {
        errors.append("\nRequired path: " + param + " in the section " + instanceSection +
                      " is missing or empty.");
      }
    }

    try {
      validateStorageAccountType(instanceSection.getStringList(
        Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES));
    } catch (IllegalArgumentException e) {
      errors.append("\n" + e.getMessage());
    }

    if (errors.length() > 0) {
      throw new RuntimeException(errors.toString().trim());
    }
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
