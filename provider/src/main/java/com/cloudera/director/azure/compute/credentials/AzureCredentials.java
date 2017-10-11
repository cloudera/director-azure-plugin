/*
 * Copyright (c) 2015 Cloudera, Inc.
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

import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials for authenticating with Azure backend. Assuming service principal style
 * authentication.
 */
public class AzureCredentials {

  private static final Logger LOG = LoggerFactory.getLogger(AzureCredentials.class);

  private final ApplicationTokenCredentials credentials;

  private final String subId;

  /**
   * Builds credentials in a backwards compatible way by:
   * 1. If the "MGMT_URL" field (plugin v1) is set then map it to the corresponding Azure Cloud and
   * build the credentials. If the field doesn't map then log an error and exit.
   * 2. Otherwise Authenticate with the "azureCloudEnvironment" field (plugin v2) normally.
   *
   * @param config config with Azure Credential fields
   * @param local localization context to extract config
   */
  public AzureCredentials(Configured config, LocalizationContext local) {
    LOG.info("Creating ApplicationTokenCredentials");

    this.subId = config.getConfigurationValue(AzureCredentialsConfiguration.SUBSCRIPTION_ID, local);
    AzureEnvironment azureEnvironment;

    // if MGMT_URL is set use it to find the AzureEnvironment
    String managementUrl =
        config.getConfigurationValue(AzureCredentialsConfiguration.MGMT_URL, local);
    if (managementUrl != null && !managementUrl.isEmpty()) {
      LOG.warn("DEPRECATION WARNING: using Management URL is deprecated. It's recommended to " +
              "REMOVE the key '{}' from the template (otherwise Director will attempt to use the " +
              "deprecated method) and specify an Azure Cloud Environment with the key '{}' " +
              "instead.",
          AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey(),
          AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT.unwrap().getConfigKey());

      azureEnvironment = AzureCloudEnvironment
          .getAzureEnvironmentFromDeprecatedConfig(managementUrl);
    } else {
      azureEnvironment = AzureCloudEnvironment.get(config
          .getConfigurationValue(AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT, local));
    }

    if (azureEnvironment == null) {
      throw new InvalidCredentialsException(String.format("Azure Cloud Environment %s is not a " +
              "valid environment. Valid environments: %s",
          config.getConfigurationValue(
              AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT, local),
          AzureCloudEnvironment.keysToString()));
    }

    this.credentials = new ApplicationTokenCredentials(
        config.getConfigurationValue(AzureCredentialsConfiguration.CLIENT_ID, local),
        config.getConfigurationValue(AzureCredentialsConfiguration.TENANT_ID, local),
        config.getConfigurationValue(AzureCredentialsConfiguration.CLIENT_SECRET, local),
        azureEnvironment);
  }

  /**
   * Returns an Azure credentials object for accessing resource management APIs in Azure.
   *
   * This returned Azure object has not been authenticated and does not authenticate with Azure
   * until it is used for a call that requires authentication (i.e. until calling a backend
   * service). This means that this method can't be used to test for valid credentials.
   *
   * xxx/all - It is unknown how long this object stays authenticated for, if it gets automatically
   * refreshed, etc
   *
   * @return base Azure object used to access resource management APIs in Azure
   */
  public Azure authenticate() {
    LOG.debug("Getting Azure API object (with deferred authentication).");
    return Azure.configure()
        .withConnectionTimeout(AzurePluginConfigHelper.getAzureSdkConnectionTimeout(),
            TimeUnit.SECONDS)
        .withReadTimeout(AzurePluginConfigHelper.getAzureSdkReadTimeout(), TimeUnit.SECONDS)
        .withMaxIdleConnections(AzurePluginConfigHelper.getAzureSdkMaxIdleConn())
        .authenticate(credentials).withSubscription(subId);
  }

  /**
   * Validates the credentials by making an Azure backend call that forces the Azure object to
   * authenticate itself immediately.
   *
   * @throws RuntimeException if there are any problems authenticating
   */
  public void validate() throws RuntimeException {
    LOG.info("Validating credentials by authenticating with Azure.");

    try {
      Azure.authenticate(credentials).withDefaultSubscription();
    } catch (Exception e) {
      LOG.error("Failed to authenticate with Azure: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns an Azure Graph RBAC manager object for AAD management.
   *
   * @return an Azure Graph RBAC manager object for AAD management
   */
  public GraphRbacManager getGraphRbacManager() {
    LOG.debug("Getting Azure Graph RBAC manager object (with deferred authentication).");
    return GraphRbacManager
        .configure()
        .withConnectionTimeout(AzurePluginConfigHelper.getAzureSdkConnectionTimeout(),
            TimeUnit.SECONDS)
        .authenticate(credentials);
  }
}
