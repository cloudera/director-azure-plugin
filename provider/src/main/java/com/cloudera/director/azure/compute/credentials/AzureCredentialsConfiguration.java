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

  // managementEndpointUrl
  MGMT_URL(new SimpleConfigurationPropertyBuilder()
    .configKey("mgmtUrl")
    .name("Management URL")
    .defaultDescription("Management URL.<br />")
    .defaultErrorMessage("Management URL is mandatory")
    .defaultValue("https://management.core.windows.net/")
    .required(true)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  // resourceManagerEndpointUrl
  ARM_URL(new SimpleConfigurationPropertyBuilder()
    .configKey("armUrl")
    .name("Resource Manager URL")
    .defaultDescription("Azure Resource Management URL.<br />")
    .defaultErrorMessage("Azure Resource Management URL is mandatory")
    .defaultValue("https://management.azure.com/")
    .required(false)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  SUBSCRIPTION_ID(new SimpleConfigurationPropertyBuilder()
    .configKey("subscriptionId")
    .name("Subscription ID")
    .defaultDescription("Azure Active Directory Subscription ID.<br />" + Constants.CREDENTIALS_INFO_LINK)
    .defaultErrorMessage("Subscription ID is mandatory")
    .required(true)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  // AAD URL should be fixed unless MSFT changes it
  AAD_URL(new SimpleConfigurationPropertyBuilder()
    .configKey("aadUrl")
    .name("AAD URL")
    .defaultDescription("Azure Active Directory URL.<br />")
    .defaultErrorMessage("AAD URL is mandatory")
    .defaultValue("https://login.windows.net/")
    .required(true)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  TENANT_ID(new SimpleConfigurationPropertyBuilder()
    .configKey("tenantId")
    .name("Tenant ID")
    .defaultDescription("Azure Active Directory Tenant ID.<br />" + Constants.CREDENTIALS_INFO_LINK)
    .defaultErrorMessage("Tenant ID is mandatory")
    .required(true)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  CLIENT_ID(new SimpleConfigurationPropertyBuilder()
    .configKey("clientId")
    .name("Client ID")
    .defaultDescription("Azure Active Directory Application Client ID.")
    .defaultErrorMessage("Client ID is mandatory")
    .required(true)
    .widget(ConfigurationProperty.Widget.TEXT)
    .build()),

  CLIENT_SECRET(new SimpleConfigurationPropertyBuilder()
    .configKey("clientSecret")
    .name("Client Secret")
    .defaultDescription("Azure Active Directory Application Client Secret.<br />" + Constants
      .CREDENTIALS_INFO_LINK)
    .sensitive(true)
    .required(true)
    .defaultErrorMessage("Client Secret is mandatory")
    .widget(ConfigurationProperty.Widget.TEXT)
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
