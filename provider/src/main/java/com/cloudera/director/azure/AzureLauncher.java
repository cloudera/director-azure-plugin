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
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

/**
 * Azure plugin launcher.
 */
public class AzureLauncher extends AbstractLauncher {

  private Config azurePluginConfig = null;
  private Config configurableImages = null;

  public AzureLauncher() {
    super(Collections.singletonList(AzureCloudProvider.METADATA), null);
  }

  /**
   * Initializes the Azure plugin by parsing plugin config file from director config directory or
   * classpath. The config file from director config directory will overwrite the one in classpath.
   * Also loads the configurable images file from the same directories. The configurable images file
   * from the director config directory will overwrite the one in classpath.
   *
   * @param configurationDirectory director configuration directory
   * @param httpProxyParameters    not used
   */
  @Override
  public void initialize(File configurationDirectory, HttpProxyParameters httpProxyParameters) {
    File configFile = new File(configurationDirectory, Configurations.AZURE_CONFIG_FILENAME);
    try {
      if (configFile.canRead()) {
        azurePluginConfig = AzurePluginConfigHelper.parseConfigFromFile(configFile);
      } else {
        azurePluginConfig = AzurePluginConfigHelper.parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);
      }
      AzurePluginConfigHelper.validatePluginConfig(azurePluginConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    File imageListFile = new File(configurationDirectory, Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
    try {
      if (imageListFile.canRead()) {
        configurableImages = AzurePluginConfigHelper.parseConfigFromFile(imageListFile);
      } else {
        configurableImages = AzurePluginConfigHelper.parseConfigFromClasspath(
          Configurations.AZURE_CONFIGURABLE_IMAGES_FILE);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a cloud provider object. Also verifies the credentials provider in config before
   * creating the provider object.
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

    LocalizationContext localizationContext = getLocalizationContext(locale);

    // Get Azure credentials
    AzureCredentialsProvider credsProvider = new AzureCredentialsProvider();
    AzureCredentials creds = credsProvider.createCredentials(configuration, localizationContext);

    // Verify the credentials by trying to get an Azure config.
    creds.createConfiguration();

    return new AzureCloudProvider(creds, azurePluginConfig, configurableImages,
      localizationContext);
  }
}
