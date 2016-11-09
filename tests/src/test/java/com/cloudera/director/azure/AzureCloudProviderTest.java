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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;


import java.util.Locale;

import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.provider.Launcher;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests to verify the expected behavior of AzureCloudProvider
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Configurations.class)
public class AzureCloudProviderTest {

  private AzureCloudProvider clouderaProvider;
  private ResourceProviderMetadata resourceProviderMetadata;
  private LocalizationContext localizationContext;
  private ConfigurationValidator validator;

  @Before
  public void setUp() {
    Launcher launcher = new AzureLauncher();
    localizationContext = launcher.getLocalizationContext(Locale.getDefault());
    resourceProviderMetadata = mock(ResourceProviderMetadata.class);
    validator = mock(ConfigurationValidator.class);
    when(resourceProviderMetadata.getProviderConfigurationValidator()).thenReturn(validator);

    PowerMockito.mockStatic(Configurations.class);

    clouderaProvider = new AzureCloudProvider(null,
      null,
      null,
      localizationContext);
  }

  @Test
  public void testSkipValidatorForAzureClouderaProvider() throws Exception {
    when(Configurations.getValidateResourcesFlag(any(Config.class))).thenReturn(false);
    assertEquals("Should get a mock Object back instead of trying to create a validate one",
      validator, clouderaProvider.getResourceProviderConfigurationValidator(
        resourceProviderMetadata));
  }

  @Test(expected = NullPointerException.class)
  public void testEnforceValidatorForAzureClouderaProvider() throws Exception {
    when(Configurations.getValidateResourcesFlag(any(Config.class))).thenReturn(true);
    //should throw NullPointerException due to validating
    clouderaProvider.getResourceProviderConfigurationValidator(resourceProviderMetadata);
  }
}
