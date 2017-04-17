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

package com.cloudera.director.azure;

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsProvider;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.common.http.HttpProxyParameters;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.util.AbstractLauncher;
import com.typesafe.config.Config;

import java.io.File;
import java.util.Collections;
import java.util.Locale;

/**
 * Azure plugin launcher.
 */
public class AzureLauncher extends AbstractLauncher {

  public AzureLauncher() {
    super(Collections.singletonList(AzureCloudProvider.METADATA), null);
  }

  /**
   * Initializes the Azure plugin by parsing two sets of config files:
   *   - azure-plugin.conf
   *   - images.conf
   * These config files are parsed as follows:
   *   1. the default config files, found in this plugin's classpath, are read and parsed
   *   2. the config values are then merged with a user-defined config file of the same name (if it
   *       exists) located in `configurationDirectory` (the director configuration directory) with
   *       the user-defined config overwriting the default config.
   *
   * @param configurationDirectory director configuration directory
   * @param httpProxyParameters    not used
   */
  @Override
  public void initialize(File configurationDirectory, HttpProxyParameters httpProxyParameters) {
    // Read in the default azure-plugin.conf and merge it with the optional user-defined
    // azure-plugin.conf file in `configurationDirectory`, with the values in the user-defined
    // azure-plugin.conf overriding the values of the default azure-plugin.conf
    Config azurePluginConfig = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIG_FILENAME,
      configurationDirectory
    );

    AzurePluginConfigHelper.validatePluginConfig(azurePluginConfig);

    AzurePluginConfigHelper.setAzurePluginConfig(azurePluginConfig);

    // Repeat the process with images.conf
    Config configurableImages = AzurePluginConfigHelper.mergeConfig(
      Configurations.AZURE_CONFIGURABLE_IMAGES_FILE,
      configurationDirectory
    );

    AzurePluginConfigHelper.setConfigurableImages(configurableImages);
  }

  /**
   * Creates a cloud provider object.
   *
   * @param cloudProviderId cloud provider id
   * @param configuration   director config
   * @param locale          used for getting localization context
   * @return an Azure cloud provider object
   */
  public CloudProvider createCloudProvider(String cloudProviderId,
    Configured configuration, Locale locale) {

    if (!AzureCloudProvider.ID.equals(cloudProviderId)) {
      throw new IllegalArgumentException("Cloud provider not found: " + cloudProviderId);
    }

    // check timeout value, throws IllegalArgumentException if value is out of range.
    checkBackendOperationPollingTimeoutFromConfig();

    LocalizationContext localizationContext = getLocalizationContext(locale);

    // Get Azure credentials
    AzureCredentialsProvider credsProvider = new AzureCredentialsProvider();
    AzureCredentials creds = credsProvider.createCredentials(configuration, localizationContext);

    return new AzureCloudProvider(creds, localizationContext);
  }

  private void checkBackendOperationPollingTimeoutFromConfig() {
    int timeout = AzurePluginConfigHelper.getAzurePluginConfigProviderSection()
      .getInt(Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS);
    int maxTimeoutValue = Configurations.TASKS_POLLING_TIMEOUT_SECONDS;
    if (timeout < 0 || timeout > maxTimeoutValue) {
      throw new IllegalArgumentException(
        String.format("plugin timeout value in second must > 0 and < %d", maxTimeoutValue)
      );
    }
  }
}
