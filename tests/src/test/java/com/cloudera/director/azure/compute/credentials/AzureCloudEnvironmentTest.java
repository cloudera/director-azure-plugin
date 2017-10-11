/*
 * Copyright (c) 2017 Cloudera, Inc.
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

import com.cloudera.director.azure.shaded.com.microsoft.azure.AzureEnvironment;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * AzureCloudEnvironment Tests
 */
public class AzureCloudEnvironmentTest {

  @After
  public void reset() throws Exception {
    // reset the map back to defaults after each test
    Map<String, AzureEnvironment> map = new HashMap<>();
    map.put(AzureCloudEnvironment.AZURE, AzureEnvironment.AZURE);
    map.put(AzureCloudEnvironment.AZURE_CHINA, AzureEnvironment.AZURE_CHINA);
    map.put(AzureCloudEnvironment.AZURE_US_GOVERNMENT, AzureEnvironment.AZURE_US_GOVERNMENT);
    map.put(AzureCloudEnvironment.AZURE_GERMANY, AzureEnvironment.AZURE_GERMANY);

    Field field =  AzureCloudEnvironment.class.getDeclaredField("azureEnvironments");
    field.setAccessible(true);
    field.set(null, map);

    Assert.assertEquals(4, AzureCloudEnvironment.size());
  }

  @Test
  public void azureCloudEnvironmentConstructorWithDefaultsExpectStaticallySetUp() throws Exception {
    Assert.assertEquals(4, AzureCloudEnvironment.size());
    Assert.assertTrue(AzureCloudEnvironment.containsKey(AzureCloudEnvironment.AZURE));
    Assert.assertTrue(AzureCloudEnvironment.containsKey(AzureCloudEnvironment.AZURE_CHINA));
    Assert.assertTrue(AzureCloudEnvironment.containsKey(AzureCloudEnvironment.AZURE_US_GOVERNMENT));
    Assert.assertTrue(AzureCloudEnvironment.containsKey(AzureCloudEnvironment.AZURE_GERMANY));
  }

  @Test
  public void putWithCustomAzureEnvironmentExpectSuccess() throws Exception {
    // create a new custom environment which is just a duplicate of Azure public
    AzureEnvironment ae = new AzureEnvironment(new HashMap<String, String>() {
      {
        put("portalUrl", "http://go.microsoft.com/fwlink/?LinkId=254433");
        put("publishingProfileUrl", "http://go.microsoft.com/fwlink/?LinkId=254432");
        put("managementEndpointUrl", "https://management.core.windows.net");
        put("resourceManagerEndpointUrl", "https://management.azure.com/");
        put("sqlManagementEndpointUrl", "https://management.core.windows.net:8443/");
        put("sqlServerHostnameSuffix", ".database.windows.net");
        put("galleryEndpointUrl", "https://gallery.azure.com/");
        put("activeDirectoryEndpointUrl", "https://login.microsoftonline.com/");
        put("activeDirectoryResourceId", "https://management.core.windows.net/");
        put("activeDirectoryGraphResourceId", "https://graph.windows.net/");
        put("activeDirectoryGraphApiVersion", "2013-04-05");
        put("storageEndpointSuffix", ".core.windows.net");
        put("keyVaultDnsSuffix", ".vault.azure.net");
        put("azureDataLakeStoreFileSystemEndpointSuffix", "azuredatalakestore.net");
        put("azureDataLakeAnalyticsCatalogAndJobEndpointSuffix", "azuredatalakeanalytics.net");
      }
    });

    AzureCloudEnvironment.put("custom", ae);

    Assert.assertEquals(5, AzureCloudEnvironment.size());
    Assert.assertTrue(ae == AzureCloudEnvironment.get("custom"));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithValidAzureConfigExpectSuccess()
      throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig(AzureEnvironment.AZURE.managementEndpoint());

    Assert.assertTrue(azureEnvironment.equals(AzureEnvironment.AZURE));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithValidAzureUsGovernmentConfigExpectSuccess()
      throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig(
            AzureEnvironment.AZURE_US_GOVERNMENT.managementEndpoint());

    Assert.assertTrue(azureEnvironment.equals((AzureEnvironment.AZURE_US_GOVERNMENT)));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithValidAzureGermanyConfigExpectSuccess()
      throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig(
            AzureEnvironment.AZURE_GERMANY.managementEndpoint());

    Assert.assertTrue(azureEnvironment.equals((AzureEnvironment.AZURE_GERMANY)));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithTrailingSlashExpectSuccess()
      throws Exception {
    AzureEnvironment azureEnvironment;
    String managementEndpointNoTrailingSlash =
        AzureEnvironment.AZURE.managementEndpoint().replaceAll("/$", "");

    // has a trailing slash
    azureEnvironment = AzureCloudEnvironment.getAzureEnvironmentFromDeprecatedConfig(
        managementEndpointNoTrailingSlash + "/");

    Assert.assertTrue(azureEnvironment.equals(AzureEnvironment.AZURE));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithoutTrailingSlashExpectSuccess()
      throws Exception {
    AzureEnvironment azureEnvironment;
    String managementEndpointNoTrailingSlash =
        AzureEnvironment.AZURE.managementEndpoint().replaceAll("/$", "");

    // no trailing slash
    azureEnvironment = AzureCloudEnvironment.getAzureEnvironmentFromDeprecatedConfig(
        managementEndpointNoTrailingSlash);

    Assert.assertTrue(azureEnvironment.equals(AzureEnvironment.AZURE));
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithInvalidAzureConfigExpectNull()
      throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig("https://fake-management.azure.com/");

    Assert.assertNull(azureEnvironment);
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithEmptyStringExpectNull() throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig("");

    Assert.assertNull(azureEnvironment);
  }

  @Test
  public void getAzureEnvironmentFromDeprecatedConfigWithNullExpectNull() throws Exception {
    AzureEnvironment azureEnvironment = AzureCloudEnvironment
        .getAzureEnvironmentFromDeprecatedConfig(null);

    Assert.assertNull(azureEnvironment);
  }
}
