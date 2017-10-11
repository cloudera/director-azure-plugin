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

import com.microsoft.azure.AzureEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translation class to map the Plugin's AzureEnvironment names to AzureEnvironments.
 */
public class AzureCloudEnvironment {
  // the default AzureEnvironment Strings
  public static final String AZURE = "azure";
  public static final String AZURE_CHINA = "azure-china";
  public static final String AZURE_US_GOVERNMENT = "azure-us-government";
  public static final String AZURE_GERMANY = "azure-germany";

  // statically initialize the default mapping
  private static Map<String, AzureEnvironment> azureEnvironments = new HashMap<>();
  static {
    azureEnvironments.put(AZURE, AzureEnvironment.AZURE);
    azureEnvironments.put(AZURE_CHINA, AzureEnvironment.AZURE_CHINA);
    azureEnvironments.put(AZURE_US_GOVERNMENT, AzureEnvironment.AZURE_US_GOVERNMENT);
    azureEnvironments.put(AZURE_GERMANY, AzureEnvironment.AZURE_GERMANY);
  }

  public static int size() {
    return azureEnvironments.size();
  }

  public static boolean containsKey(String key) {
    return azureEnvironments.containsKey(key);
  }

  public static boolean containsValue(AzureEnvironment value) {
    return azureEnvironments.containsValue(value);
  }

  public static AzureEnvironment get(String key) {
    return azureEnvironments.get(key);
  }

  public static AzureEnvironment put(String key, AzureEnvironment ae) {
    return azureEnvironments.put(key, ae);
  }

  public static List<String> keys() {
    return new ArrayList<>(azureEnvironments.keySet());
  }

  /**
   * Gets the alphabetically sorted keys as a String meant to be printed.
   *
   * @return the keys as a String
   */
  public static String keysToString() {
    List<String> keys = keys();
    Collections.sort(keys);

    StringBuilder sb = new StringBuilder();
    sb.append("[");

    for (int i = 0; i < keys.size() - 1; i += 1) {
      sb.append(keys.get(i));
      sb.append(", ");
    }

    sb.append(keys.get(keys.size() - 1));
    sb.append("]");

    return sb.toString();
  }

  public static List<AzureEnvironment> values() {
    return new ArrayList<>(azureEnvironments.values());
  }

  /**
   * Gets the corresponding AzureEnvironment from the deprecated plugin v1 management url.
   *
   * N.b. that Azure China is currently not supported.
   *
   * @param managementUrl plugin v1 endpoint
   * @return the AzureEnvironment or null if it doesn't exist
   */
  static AzureEnvironment getAzureEnvironmentFromDeprecatedConfig(String managementUrl) {
    if (managementUrl == null) {
      return null;
    }

    managementUrl = managementUrl.replaceAll("/$", "");

    if (managementUrl.equals(AzureEnvironment.AZURE.managementEndpoint().replaceAll("/$", ""))) {
      return AzureEnvironment.AZURE;
    } else if (managementUrl.equals(AzureEnvironment.AZURE_US_GOVERNMENT.managementEndpoint()
        .replaceAll("/$", ""))) {
      return AzureEnvironment.AZURE_US_GOVERNMENT;
    } else if (managementUrl.equals(AzureEnvironment.AZURE_GERMANY.managementEndpoint()
        .replaceAll("/$", ""))) {
      return AzureEnvironment.AZURE_GERMANY;
    }
    // Azure China is currently not supported

    // the deprecated config doesn't match any AzureEnvironment; return null
    return null;
  }
}
