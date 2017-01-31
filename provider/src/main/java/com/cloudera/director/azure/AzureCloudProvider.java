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
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import java.util.Collections;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure cloud provider plugin
 */
public class AzureCloudProvider extends AbstractCloudProvider {

  private AzureCredentials credentials;

  private static final Logger LOG = LoggerFactory.getLogger(AzureCloudProvider.class);

  /**
   * The cloud provider ID.
   */
  public static final String ID = "azure";
  public static final CloudProviderMetadata METADATA =
    new SimpleCloudProviderMetadataBuilder()
      .id(ID)
      .name("Microsoft Azure Cloud Platform")
      .description("A provider implementation for provisioning virtual resources on Microsoft Azure " +
        "Cloud Platform.")
      .configurationProperties(Collections.<ConfigurationProperty>emptyList())
      .credentialsProviderMetadata(AzureCredentialsProvider.METADATA)
      .resourceProviderMetadata(Collections.singletonList(AzureComputeProvider.METADATA))
      .build();

  public AzureCloudProvider(AzureCredentials creds, LocalizationContext rootLocalizationContext) {
    super(METADATA, rootLocalizationContext);
    this.credentials = creds;
  }

  public ResourceProvider createResourceProvider(String resourceProviderId, Configured configuration) {
    if (AzureComputeProvider.METADATA.getId().equals(resourceProviderId)) {
      // AZURE_SDK Azure SDK requires the following calls to correctly create clients.
      ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(ManagementConfiguration.class.getClassLoader());
      try {
        return new AzureComputeProvider(configuration, credentials, getLocalizationContext());
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        Thread.currentThread().setContextClassLoader(contextLoader);
      }
    }
    throw new NoSuchElementException("Invalid provider id: " + resourceProviderId);
  }

  @Override
  protected ConfigurationValidator getResourceProviderConfigurationValidator(
    ResourceProviderMetadata resourceProviderMetadata) {
    if (!AzurePluginConfigHelper.getValidateResourcesFlag()) {
      LOG.info("Skip all compute provider validator checks.");
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
