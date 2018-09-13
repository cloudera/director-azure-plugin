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

import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.provider.CredentialsProvider;
import com.cloudera.director.spi.v2.provider.CredentialsProviderMetadata;
import com.cloudera.director.spi.v2.provider.util.SimpleCredentialsProviderMetadata;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;

import java.util.List;

/**
 * Convert director config to Azure credentials
 */
public class AzureCredentialsProvider implements CredentialsProvider<AzureCredentials> {
  private static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil
          .asConfigurationPropertyList(AzureCredentialsConfiguration.values());

  public static final CredentialsProviderMetadata METADATA =
      new SimpleCredentialsProviderMetadata(CONFIGURATION_PROPERTIES);

  public CredentialsProviderMetadata getMetadata() {
    return METADATA;
  }

  public AzureCredentials createCredentials(Configured configuration,
      LocalizationContext localizationContext) {
    return new AzureCredentials(configuration, localizationContext);
  }
}
