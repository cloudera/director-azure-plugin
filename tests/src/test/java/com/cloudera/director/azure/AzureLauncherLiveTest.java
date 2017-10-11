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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v1.provider.Launcher;

import java.io.File;
import java.util.List;
import java.util.Locale;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * AzureLauncher live tests.
 *
 * These tests do not try to authenticate with Azure, only set up the objects. See
 * AzureCloudProviderLiveTest for tests that authenticate.
 */
public class AzureLauncherLiveTest {

  @BeforeClass
  public static void checkLiveTestFlag() {
    assumeTrue(TestHelper.runLiveTests());
  }

  @Test
  public void createCloudProviderWithValidFieldsExpectSuccess() throws Exception {
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    assertEquals(1, launcher.getCloudProviderMetadata().size());

    CloudProviderMetadata metadata = launcher.getCloudProviderMetadata().get(0);
    assertEquals(AzureCloudProvider.ID, metadata.getId());

    List<ConfigurationProperty> providerConfigurationProperties = metadata
        .getProviderConfigurationProperties();
    assertEquals(0, providerConfigurationProperties.size());

    // See AzureCredentialsConfiguration
    List<ConfigurationProperty> credentialsConfigurationProperties = metadata
        .getCredentialsProviderMetadata().getCredentialsConfigurationProperties();
    assertEquals(6, credentialsConfigurationProperties.size());
    assertTrue(credentialsConfigurationProperties.contains(
        AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(
        AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(
        AzureCredentialsConfiguration.TENANT_ID.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(
        AzureCredentialsConfiguration.CLIENT_ID.unwrap()));
    assertTrue(credentialsConfigurationProperties.contains(
        AzureCredentialsConfiguration.CLIENT_SECRET.unwrap()));

    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    assertEquals(AzureCloudProvider.class, cloudProvider.getClass());
  }
}
