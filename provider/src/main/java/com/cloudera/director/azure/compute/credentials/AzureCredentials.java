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

import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.AAD_URL;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.ARM_URL;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.CLIENT_ID;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.CLIENT_SECRET;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.MGMT_URL;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.SUBSCRIPTION_ID;
import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.TENANT_ID;

import com.cloudera.director.azure.compute.provider.AzureComputeProviderHelper;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v1.model.exception.TransientProviderException;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.microsoft.azure.utility.AuthHelper;
import com.microsoft.azure.utility.ResourceContext;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.credentials.TokenCloudCredentials;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import javax.naming.ServiceUnavailableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials for authenticating with Azure backend. Assuming service principle
 * style authentication.
 * <p/>
 * To authenticate with Azure backend, we need to have:
 * - Management URL
 * - Azure AD URL
 * - Tenant ID
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class AzureCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(AzureCredentials.class);

  private final String subId;
  private final String mgmtUrl;
  private final String armUrl;
  private final String aadUrl;
  private final String tenant;
  private final String clientId;
  private final String clientKey;

  public AzureCredentials(Configured config, LocalizationContext local) {
    this.subId = config.getConfigurationValue(SUBSCRIPTION_ID, local);
    this.mgmtUrl = config.getConfigurationValue(MGMT_URL, local);
    this.armUrl = config.getConfigurationValue(ARM_URL, local);
    this.aadUrl = config.getConfigurationValue(AAD_URL, local);
    this.tenant = config.getConfigurationValue(TENANT_ID, local);
    this.clientId = config.getConfigurationValue(CLIENT_ID, local);
    this.clientKey = config.getConfigurationValue(CLIENT_SECRET, local);
  }

  public AzureCredentials(String subId, String mgmtUrl, String armUrl, String aadUrl,
    String tenant, String clientId, String clientKey) {
    this.subId = subId;
    this.mgmtUrl = mgmtUrl;
    this.armUrl = armUrl;
    this.aadUrl = aadUrl;
    this.tenant = tenant;
    this.clientId = clientId;
    this.clientKey = clientKey;
  }

  /**
   * Creates CloudCredentials.
   *
   * NOTE: AccessToken will expire. When it does, user must request for a new one.
   *
   * @return Token based cloud credentials
   * @throws ServiceUnavailableException Something broke when making a call to Azure Active
   *                                     Directory.
   * @throws MalformedURLException       The url provided to AAD was not properly formed.
   * @throws ExecutionException          Something went wrong.
   * @throws InterruptedException        The request to AAD has been interrupted.
   */
  private TokenCloudCredentials createCredentials()
    throws ServiceUnavailableException, MalformedURLException, ExecutionException,
    InterruptedException {
    return new TokenCloudCredentials(null, subId,
      AuthHelper.getAccessTokenFromServicePrincipalCredentials(
        mgmtUrl, aadUrl, tenant, clientId, clientKey).getAccessToken());
  }

  /**
   * Creates configuration builds the management configuration needed for creating the clients.
   * <p/>
   * There's some trickiness around URIs (see code for more details):
   *   - under the hood the SDK uses the management URI correctly only for getting an access token
   *     (see getAccessTokenFromServicePrincipalCredentials())
   *   - even though it's passed in when we create the Configuration object, the URI is not being
   *     used to set the "management.uri" field
   *   - within the Configuration and underlying Client SDK classes "management.uri" is used where
   *     the Azure Resource Manager (ARM) URI should be; to fix this, we explicitly set
   *     "management.uri" to ARM URL so that references of "management.uri" get the correct URL
   * <p/>
   * The config contains the baseURI which is the base of the ARM REST service,
   * the subscription id as the context for the ResourceManagementService and
   * the AAD token required for the HTTP Authorization header.
   * <p/>
   * A new configuration must be created once AccessToken expires.
   *
   * @return the generated configuration
   * @throws UnrecoverableProviderException hit problems with AAD service
   * @throws TransientProviderException     call to AAD backend interrupted
   */
  public Configuration createConfiguration() throws UnrecoverableProviderException,
    TransientProviderException {
    LOG.info("Initializing Configuration with \"management.uri\" as " + armUrl);

    try {
      Configuration configuration = ManagementConfiguration.configure(
        null, // AZURE_SDK: This is always set to null in all the examples in Azure SDK
        // AZURE_SDK: this does not set the "management.uri" field, it sets a field that is never
        // referenced - essentially this can be anything
        new URI(armUrl),
        subId,
        createCredentials().getToken());
      // AZURE_SDK: The "management.uri" field is used as if it was actually ARM URI
      configuration.setProperty("management.uri", new URI(armUrl));
      return configuration;
    } catch (URISyntaxException | MalformedURLException e) {
      LOG.error("Malformed Azure Resource Manager URL (stored in \"management.uri\"): {}.",
        armUrl, e);
      throw new InvalidCredentialsException(e);
    } catch (ServiceUnavailableException | IOException e) {
      LOG.error("Encountered error contacting Azure Active Directory service.", e);
      throw new UnrecoverableProviderException(e);
    } catch (ExecutionException e) {
      LOG.error("Invalid credentials.", e);
      throw new InvalidCredentialsException(e);
    } catch (InterruptedException e) {
      LOG.error("The request to Azure Active Director has been interrupted.", e);
      throw new TransientProviderException(e);
    }
  }

  public String getSubscriptionId() {
    return subId;
  }

  public String getManagemetUrl() {
    return mgmtUrl;
  }

  public String getAadUrl() {
    return aadUrl;
  }

  /**
   * Create an empty Azure resource context with the subscription ID.
   *
   * @param location      Azure "Region"
   * @param resourceGroup resource group name
   * @param publicIpFlag  true if public IP is requested
   * @return an empty Azure resource context with the subscription ID
   */
  public ResourceContext createResourceContext(String location, String resourceGroup, boolean publicIpFlag) {
    return new ResourceContext(location, resourceGroup, subId, publicIpFlag);
  }

  /**
   * @return an AzureComputeProviderHelper object
   */
  public AzureComputeProviderHelper getComputeProviderHelper() {
    // AZURE_SDK Azure SDK requires the following calls to correctly create clients.
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ManagementConfiguration.class.getClassLoader());
    try {
      return new AzureComputeProviderHelper(createConfiguration());
    } finally {
      Thread.currentThread().setContextClassLoader(contextLoader);
    }
  }
}
