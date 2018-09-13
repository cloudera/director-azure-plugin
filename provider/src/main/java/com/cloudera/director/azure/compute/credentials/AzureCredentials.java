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

import static com.cloudera.director.azure.Configurations.AZURE_USER_AGENT_PREFIX;

import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.google.common.base.Strings;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.graphrbac.implementation.GraphRbacManager;
import com.microsoft.azure.management.msi.implementation.MSIManager;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
  private final String userAgentPid;
  private static final String UUID_REGEX =
      "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";
  private static final Pattern UUID_PATTERN = Pattern.compile(UUID_REGEX);

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

    // set to the Cloudera Altus Director user-agent GUID by default
    String userAgent = config.getConfigurationValue(AzureCredentialsConfiguration.USER_AGENT, local);

    // validate the user-agent GUID
    if (!Strings.isNullOrEmpty(userAgent) && UUID_PATTERN.matcher(userAgent).matches()) {
      // set the user-agent if it's a valid GUID
      this.userAgentPid = AZURE_USER_AGENT_PREFIX + userAgent;
    } else if (!Strings.isNullOrEmpty(userAgent)) {
      // error if the user-agent is set and invalid
      throw new InvalidCredentialsException(String.format(
          "The user-agent GUID %s is not a valid GUID; it must match the regex: %s",
          userAgent,
          UUID_REGEX));
    } else {
      // if the user-agent was purposefully set to empty-string then set it blank
      userAgentPid = "";
    }

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
   * N.b. it is unknown how long this object stays authenticated for, if it gets automatically
   * refreshed, etc
   *
   * @return base Azure object used to access resource management APIs in Azure
   */
  public Azure authenticate() {
    return Azure.configure()
        .withUserAgent(userAgentPid)
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
   * Returns an MSIManager object for accessing Managed Service Identity APIs in Azure.
   *
   * This returned MSIManager object is similar to the Azure object: it has not been authenticated and does not
   * authenticate with Azure until it is used for a call that requires authentication (i.e. until calling a backend
   * service). This means that this method can't be used to test for valid credentials.
   *
   * To validate that credentials are correct call the validate() method.
   *
   * @return base MSIManager object used to access MSI APIs in Azure
   */
  public MSIManager getMsiManager() {
    return MSIManager.authenticate(credentials, subId);
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
