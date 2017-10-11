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
import com.cloudera.director.azure.compute.provider.AzureComputeProvider;
import com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationValidator;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v1.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v1.provider.ResourceProvider;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.AbstractCloudProvider;
import com.cloudera.director.spi.v1.provider.util.SimpleCloudProviderMetadataBuilder;

import java.util.Collections;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure cloud provider plugin
 */
public class AzureCloudProvider extends AbstractCloudProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AzureCloudProvider.class);

  /**
   * The cloud provider ID.
    */
  public static final String ID = "azure";

  static final CloudProviderMetadata METADATA = new SimpleCloudProviderMetadataBuilder()
      .id(ID)
      .name("Microsoft Azure")
      .description("A provider implementation that provisions virtual resources on Microsoft " +
          "Azure.")
      .configurationProperties(Collections.<ConfigurationProperty>emptyList())
      .credentialsProviderMetadata(AzureCredentialsProvider.METADATA)
      .resourceProviderMetadata(Collections.singletonList(AzureComputeProvider.METADATA))
      .build();

  private AzureCredentials credentials;

  public AzureCloudProvider(AzureCredentials credentials,
      LocalizationContext rootLocalizationContext) {
    super(METADATA, rootLocalizationContext);
    this.credentials = credentials;
  }

  /**
   * Creates an AzureComputeProvider object. Also verifies the credentials provided in config unless
   * the flag to validate credentials is false.
   *
   * @param resourceProviderId the resource provider ID (should be the azure ID)
   * @param configuration resource provider configuration
   * @return and AzureComputeProvider object
   * @throws RuntimeException if credentials are invalid
   */
  @Override
  public ResourceProvider createResourceProvider(String resourceProviderId,
      Configured configuration) {
    // check that this is really the Azure provider ID
    if (!AzureComputeProvider.METADATA.getId().equals(resourceProviderId)) {
      throw new NoSuchElementException("Invalid provider id: " + resourceProviderId);
    }

    if (AzurePluginConfigHelper.validateCredentials()) {
      // validate credentials by trying to authenticate with Azure
      credentials.validate();
    } else {
      LOG.info("Skipping Azure credential validation with Azure backend.");
    }

    return new AzureComputeProvider(configuration, credentials, getLocalizationContext());
  }

  @Override
  protected ConfigurationValidator getResourceProviderConfigurationValidator(
      ResourceProviderMetadata resourceProviderMetadata) {
    if (!AzurePluginConfigHelper.validateResources()) {
      LOG.info("Skipping all compute provider validator checks.");
      return resourceProviderMetadata.getProviderConfigurationValidator();
    }

    ConfigurationValidator providerSpecificValidator;
    if (resourceProviderMetadata.getId().equals(AzureComputeProvider.METADATA.getId())) {
      providerSpecificValidator = new AzureComputeProviderConfigurationValidator();
    } else {
      throw new NoSuchElementException("Invalid provider id: " + resourceProviderMetadata.getId());
    }
    return new CompositeConfigurationValidator(METADATA.getProviderConfigurationValidator(),
        providerSpecificValidator);
  }
}
