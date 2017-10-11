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

package com.cloudera.director.azure.compute.credentials;

import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;

/**
 * Configuration properties needed to create AzureCredentials.
 */
public enum AzureCredentialsConfiguration implements ConfigurationPropertyToken {

  AZURE_CLOUD_ENVIRONMENT(new SimpleConfigurationPropertyBuilder()
      .configKey("azureCloudEnvironment")
      .name("Azure Cloud Environment")
      .addValidValues(
          AzureCloudEnvironment.AZURE,
          AzureCloudEnvironment.AZURE_US_GOVERNMENT,
          AzureCloudEnvironment.AZURE_GERMANY)
      .defaultValue(AzureCloudEnvironment.AZURE)
      .defaultDescription("The Azure Cloud to use.")
      .defaultErrorMessage("The Azure Cloud is required.")
      .required(false)
      .widget(ConfigurationProperty.Widget.OPENLIST)
      .build()),

  SUBSCRIPTION_ID(new SimpleConfigurationPropertyBuilder()
      .configKey("subscriptionId")
      .name("Subscription ID")
      .defaultDescription("Azure Active Directory Subscription ID.")
      .defaultErrorMessage("Subscription ID is required.")
      .required(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  TENANT_ID(new SimpleConfigurationPropertyBuilder()
      .configKey("tenantId")
      .name("Tenant ID")
      .defaultDescription("Azure Active Directory Tenant ID.")
      .defaultErrorMessage("Tenant ID is required.")
      .required(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  CLIENT_ID(new SimpleConfigurationPropertyBuilder()
      .configKey("clientId")
      .name("Client ID")
      .defaultDescription("Azure Active Directory Application Client ID.")
      .defaultErrorMessage("Client ID is required.")
      .required(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  CLIENT_SECRET(new SimpleConfigurationPropertyBuilder()
      .configKey("clientSecret")
      .name("Client Secret")
      .defaultDescription("Azure Active Directory Application Client Secret.")
      .defaultErrorMessage("Client Secret is required.")
      .sensitive(true)
      .required(true)
      .widget(ConfigurationProperty.Widget.TEXT)
      .build()),

  MGMT_URL(new SimpleConfigurationPropertyBuilder()
      .configKey("mgmtUrl")
      .name("(DEPRECATED) Management URL")
      .defaultDescription("(DEPRECATED) Management URL; use Azure Cloud Environment instead.")
      .defaultErrorMessage("Management URL is DEPRECATED; use Azure Cloud Environment instead.")
      .required(false)
      .hidden(true)
      .build());

  private final ConfigurationProperty configProperty;

  /**
   * Creates a configuration property token with the specified parameters.
   *
   * @param configurationProperty the configuration property
   */
  AzureCredentialsConfiguration(ConfigurationProperty configurationProperty) {
    this.configProperty = configurationProperty;
  }

  public ConfigurationProperty unwrap() {
    return configProperty;
  }
}
