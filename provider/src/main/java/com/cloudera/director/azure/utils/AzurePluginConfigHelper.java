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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

import java.io.File;
import java.io.IOException;

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
  }
}
