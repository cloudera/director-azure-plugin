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

/**
 * Constants for important properties and sections in the configuration file
 *
 * @see <a href="https://github.com/typesafehub/config" />
 */
public final class Configurations {

  private Configurations() {
  }

  /**
   * The configuration file name.
   */
  public static final String AZURE_CONFIG_FILENAME = "azure-plugin.conf";

  /**
   * Name of different sections Azure Director plugin configuration file
   */
  public static final String AZURE_CONFIG_PROVIDER = "provider";
  public static final String AZURE_CONFIG_PROVIDER_REGIONS = "supported-regions";

  public static final String AZURE_CONFIG_INSTANCE = "instance";
  public static final String AZURE_CONFIG_INSTANCE_SUPPORTED = "supported-instances";
  public static final String AZURE_CONFIG_INSTANCE_DNS_LABEL_REGEX = "instance-prefix-regex";
  public static final String AZURE_CONFIG_INSTANCE_FQDN_SUFFIX_REGEX = "dns-fqdn-suffix-regex";

  /**
   * File containing list of configurable images.
   */
  public static final String AZURE_CONFIGURABLE_IMAGES_FILE = "images.conf";

  /**
   * Elements that specifies am Azure VM Image, used as config keys to parse the configurable
   * images file.
   */
  public static final String AZURE_IMAGE_PUBLISHER = "publisher";
  public static final String AZURE_IMAGE_OFFER = "offer";
  public static final String AZURE_IMAGE_SKU = "sku";
  public static final String AZURE_IMAGE_VERSION = "version";
}
