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

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_PROVIDER_REGIONS;
import static com.cloudera.director.spi.v1.model.util.Validations.addError;

import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies AzureComputeProvider configuration to make sure user selected a region that supports
 * Premium Storage.
 */
public class AzureComputeProviderConfigurationValidator implements ConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProviderConfigurationValidator.class);

  private Config pluginConfig;
  static final String REGION_NOT_SUPPORTED_MSG = "Region '%s' does not support premium storage.";

  public AzureComputeProviderConfigurationValidator(Config cfg) {
    this.pluginConfig = cfg;
  }

  @Override
  public void validate(String name, Configured configuration, PluginExceptionConditionAccumulator accumulator,
    LocalizationContext localizationContext) {
    checkPremiumStorage(configuration, accumulator, localizationContext);
  }

  /**
   * Check to make sure user provided region is supports premium storage.
   *
   * @param directorConfig      Director config
   * @param accumulator         error accumulator
   * @param localizationContext localization context to extract config
   */
  void checkPremiumStorage(Configured directorConfig, PluginExceptionConditionAccumulator accumulator,
    LocalizationContext localizationContext) {
    String regionName =
      directorConfig.getConfigurationValue(AzureComputeProviderConfigurationProperty.REGION, localizationContext);
    if (!pluginConfig.getStringList(AZURE_CONFIG_PROVIDER_REGIONS).contains(regionName)) {
      LOG.error("Region '" + regionName + "' does not support premium storage.");
      addError(accumulator, AzureComputeProviderConfigurationProperty.REGION, localizationContext, null,
        REGION_NOT_SUPPORTED_MSG, regionName);
    }
  }
}
