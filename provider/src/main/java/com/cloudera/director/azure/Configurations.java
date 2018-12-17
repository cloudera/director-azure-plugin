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

import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.ValidationException;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.compute.PurchasePlan;
import com.microsoft.azure.management.storage.SkuName;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constants for important properties and sections in the configuration files.
 */
public final class Configurations {

  /**
   * The user-agent GUID must be in this form:
   *   pid-GUID
   * The "pid-" prefix is required from the Microsoft side.
   */
  public static final String AZURE_USER_AGENT_PREFIX = "pid-";

  /**
   * The Cloudera Altus Director user-agent GUID used to track usage from Cloudera.
   */
  public static final String AZURE_DEFAULT_USER_AGENT_GUID = "c1d2dcc3-5b9d-46b7-a678-8c8f982a08f8";

  /**
   * The configuration file name.
   */
  public static final String AZURE_CONFIG_FILENAME = "azure-plugin.conf";

  /**
   * Name of different sections Azure Director plugin configuration file
   */
  public static final String AZURE_CONFIG_PROVIDER = "provider";
  public static final String AZURE_CONFIG_PROVIDER_REGIONS = "supported-regions";
  public static final String AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS =
      "azure-backend-operation-polling-timeout-second";
  public static final String AZURE_CONFIG_PROVIDER_VMSS_OPERATION_TIMEOUT_SECONDS =
      "azure-vmss-operation-timeout-second";
  public static final int MAX_TASKS_POLLING_TIMEOUT_SECONDS = 3600;
  public static final String AZURE_SDK_CONFIG_CONN_TIMEOUT_SECONDS =
      "azure-sdk-connection-timeout-seconds";
  public static final String AZURE_SDK_CONFIG_READ_TIMEOUT_SECONDS =
      "azure-sdk-read-timeout-seconds";
  public static final String AZURE_SDK_CONFIG_MAX_IDLE_CONN = "azure-sdk-max-idle-connections";
  public static final String AZURE_HOST_KEY_FINGERPRINT_COMMAND = "azure-host-key-fingerprint-command";

  public static final String AZURE_CONFIG_INSTANCE = "instance";
  public static final String AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES =
      "supported-storage-account-types";
  public static final String AZURE_CONFIG_INSTANCE_MAXIMUM_DISK_SIZE =
      "maximum-disk-size";
  public static final String AZURE_CONFIG_INSTANCE_PREFIX_REGEX = "instance-prefix-regex";
  public static final String AZURE_CONFIG_INSTANCE_FQDN_SUFFIX_REGEX = "dns-fqdn-suffix-regex";
  public static final String AZURE_CONFIG_DISALLOWED_USERNAMES = "azure-disallowed-usernames";
  public static final String AZURE_VALIDATE_RESOURCES = "azure-validate-resources";
  public static final String AZURE_VALIDATE_CREDENTIALS = "azure-validate-credentials";
  public static final int AZURE_CUSTOM_DATA_MAX_CHARACTERS = 87380;

  /**
   * The configurable images file name.
   */
  public static final String AZURE_CONFIGURABLE_IMAGES_FILE = "images.conf";

  /**
   * Elements that specifies am Azure VM Image, used as config keys to parse the configurable
   * images file.
   */
  private static final String AZURE_IMAGE_PUBLISHER = "publisher";
  private static final String AZURE_IMAGE_OFFER = "offer";
  private static final String AZURE_IMAGE_SKU = "sku";
  private static final String AZURE_IMAGE_VERSION = "version";
  private static final String AZURE_CUSTOM_IMAGE_MARKER = "providers/Microsoft.Compute/images";

  /**
   * Defaults for Azure VM properties
   */
  public static final String AZURE_DEFAULT_STORAGE_TYPE = SkuName.PREMIUM_LRS.toString();
  // the P30 disk is 1024
  public static final String AZURE_DEFAULT_DATA_DISK_SIZE = "1024";

  /**
   * A Map of the old (Azure SDK v0.9) Storage Account Type strings to the current (Azure SDK v1)
   * Storage Account Type strings.
   */
  public static final Map<String, String> DEPRECATED_STORAGE_ACCOUNT_TYPES;
  static {
    Map<String, String> deprecatedStorageAccountTypes = new HashMap<>();
    deprecatedStorageAccountTypes.put("PremiumLRS", SkuName.PREMIUM_LRS.toString());
    deprecatedStorageAccountTypes.put("StandardLRS", SkuName.STANDARD_LRS.toString());
    DEPRECATED_STORAGE_ACCOUNT_TYPES = Collections.unmodifiableMap(deprecatedStorageAccountTypes);
  }

  /**
   * Maps an old (SDK v0.9) Storage Account Type string to a current (SDK v1) Storage Account Type.
   *
   * @param storageAccountType the Storage Account Type to map
   * @return the current version or, if the mapping doesn't exist, the original string
   */
  public static String convertStorageAccountTypeString(String storageAccountType) {
    return DEPRECATED_STORAGE_ACCOUNT_TYPES.containsKey(storageAccountType) ?
        DEPRECATED_STORAGE_ACCOUNT_TYPES.get(storageAccountType) :
        storageAccountType;
  }

  /**
   * Gets the required provider fields. Currently all provider fields are required.
   *
   * @return the list of required provider fields
   */
  public static List<String> getRequiredProviderFields() {
    return Arrays.asList(
        Configurations.AZURE_CONFIG_PROVIDER_REGIONS,
        Configurations.AZURE_CONFIG_PROVIDER_BACKEND_OPERATION_POLLING_TIMEOUT_SECONDS);
  }

  /**
   * Gets the required instance fields. Currently all instance fields are required.
   *
   * @return the list of required instance fields
   */
  public static List<String> getRequiredInstanceFields() {
    return Arrays.asList(
        Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES,
        Configurations.AZURE_CONFIG_INSTANCE_MAXIMUM_DISK_SIZE,
        Configurations.AZURE_CONFIG_INSTANCE_PREFIX_REGEX,
        Configurations.AZURE_CONFIG_INSTANCE_FQDN_SUFFIX_REGEX,
        Configurations.AZURE_CONFIG_DISALLOWED_USERNAMES);
  }

  private static final String CUSTOM_IMAGE_PLAN_PUBLISHER_KEY = "publisher";
  private static final String CUSTOM_IMAGE_PLAN_PRODUCT_KEY = "product";
  private static final String CUSTOM_IMAGE_PLAN_NAME_KEY = "name";

  /**
   * Parses the image fields from the VM image string as one of:
   * a. (recommended) an Image URN in this format:
   *    publisher:offer:sku:version
   *    See https://docs.microsoft.com/en-us/azure/virtual-machines/linux/cli-ps-findimage#terminology
   * b. (deprecated) a one line representation of an image in this format (this is a Cloudera created format):
   *    /publisher/<publisher>/offer/<offer>/sku/<sku>/version/<version>
   * c. (not recommended) the name of an image specified in the configurable images file.
   *
   * @param template instance config
   * @param localizationContext localization context
   * @return a map of the four image fields
   * @throws ValidationException when the VM image string is invalid
   */
  public static ImageReference parseImageFromConfig(final Configured template,
      LocalizationContext localizationContext) throws ValidationException {
    final String imageNotUrnMsg = "Image '%s' has the correct URN structure but not all fields are set. " +
        "The URN structure looks like: '<publisher>:<offer>:<sku>:<version>'.";
    final String imageNotInlineMsg = "Image '%s' has the correct (deprecated) inline structure but does not follow " +
        "the inline format of: '/publisher/<publisher>/offer/<offer>/sku/<sku>/version/<version>'.";
    final String imageMissingInConfigMsg = "Image '%s' does not exist in configurable image list.";
    final String imageConfigMissingRequiredFieldMsg = "Image '%s' config does not have all " +
        "required fields or fields are the wrong type. Check the configurable images file.";
    final String imageMightBeCustom = "Image '%s' is referencing a Custom Image but not all required fields are set. " +
        "To use Custom Images set the '" +
        AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap().getConfigKey() +
        "' and '" +
        AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap().getConfigKey() +
        "' fields.";

    String imageString = template.getConfigurationValue(
        ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.IMAGE,
        localizationContext);

    // they're trying to use a custom image if this is set
    if (imageString.contains(AZURE_CUSTOM_IMAGE_MARKER)) {
      throw new ValidationException(String.format(imageMightBeCustom, imageString));
    }

    // see if the image follows the URN format
    String[] urnSplitPath = imageString.split(":");
    if (urnSplitPath.length == 4) {
      // the image is in URN form, build it

      // URN form: publisher:offer:sku:version
      String publisher = urnSplitPath[0];
      String offer = urnSplitPath[1];
      String sku = urnSplitPath[2];
      String version = urnSplitPath[3];

      if (publisher == null || offer == null || sku == null || version == null) {
        throw new ValidationException(String.format(imageNotUrnMsg, imageString));
      }

      return new ImageReference()
          .withPublisher(publisher)
          .withOffer(offer)
          .withSku(sku)
          .withVersion(version);
    }

    // the image string is not a URN - see if the image follows the deprecated inline format
    String[] inlineSplitPath = imageString.split("/");
    if (inlineSplitPath.length == 9) {
      // the image is in URI form, build it
      Map<String, String> imageMap = new HashMap<>();
      for (int i = 1; i < inlineSplitPath.length; i += 2) {
        imageMap.put(inlineSplitPath[i], inlineSplitPath[i + 1]);
      }

      String publisher = imageMap.get(Configurations.AZURE_IMAGE_PUBLISHER);
      String offer = imageMap.get(Configurations.AZURE_IMAGE_OFFER);
      String sku = imageMap.get(Configurations.AZURE_IMAGE_SKU);
      String version = imageMap.get(Configurations.AZURE_IMAGE_VERSION);

      if (publisher == null || offer == null || sku == null || version == null) {
        throw new ValidationException(String.format(imageNotInlineMsg, imageString));
      }

      return new ImageReference()
          .withPublisher(publisher)
          .withOffer(offer)
          .withSku(sku)
          .withVersion(version);
    }

    // the image string is not a URN or an inline - see if the image exists in images.conf
    Config image;
    try {
      image = AzurePluginConfigHelper.getConfigurableImages().getConfig(imageString);
    } catch (ConfigException e) {
      throw new ValidationException(String.format(imageMissingInConfigMsg, imageString));
    }

    // the image string references an image in images.conf - see if that image has the right fields
    try {
      // the image exists in images.conf, try to build the image with its fields
      return new ImageReference()
          .withPublisher(image.getString(Configurations.AZURE_IMAGE_PUBLISHER))
          .withOffer(image.getString(Configurations.AZURE_IMAGE_OFFER))
          .withSku(image.getString(Configurations.AZURE_IMAGE_SKU))
          .withVersion(image.getString(Configurations.AZURE_IMAGE_VERSION));
    } catch (ConfigException e) {
      throw new ValidationException(String.format(imageConfigMissingRequiredFieldMsg, imageString));
    }
  }

  /**
   * Tests if the ImageReference is a preview image.
   *
   * An image is a preview image if the offer field ends with "-preview".
   *
   * @param imageReference the image
   * @return true if the image is a preview image; false otherwise
   */
  public static boolean isPreviewImage(ImageReference imageReference) {
    try {
      return imageReference.offer().endsWith("-preview");
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Parses and constructs a custom image purchase plan from instance config. The custom image plan
   * config must have the format: /publisher/<value>/product/<value>/name/<value>
   *
   * @param template instance config
   * @param templateLocalizationContext localization context
   * @return null when no purchase plan is specified or a constructed PurchasePlan object
   * @throws ValidationException when the custom image purchase plan config is not valid
   */
  public static PurchasePlan parseCustomImagePurchasePlanFromConfig(final Configured template,
      LocalizationContext templateLocalizationContext) throws ValidationException {
    final String customImagePlan = template.getConfigurationValue(
        AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN,
        templateLocalizationContext);
    final String invalidConfigErrMsg = "Invalid Custom Image Purchase Plan configuration: %s. " +
        "Allowed configs are 1) <empty string> (for no plan specified); or 2) string with the " +
        "format: /publisher/<publisher>/product/<product>/name/<name>.";

    if (customImagePlan == null || customImagePlan.isEmpty()) {
      return null;
    }

    String[] splitPath = customImagePlan.split("/");
    if (splitPath.length != 7) {
      throw new ValidationException(String.format(invalidConfigErrMsg, customImagePlan));
    }

    Map<String, String> customImagePlanCfgMap = new HashMap<>();
    for (int i = 1; i < splitPath.length; i += 2) {
      customImagePlanCfgMap.put(splitPath[i], splitPath[i + 1]);
    }

    String name = customImagePlanCfgMap.get(CUSTOM_IMAGE_PLAN_NAME_KEY);
    String product = customImagePlanCfgMap.get(CUSTOM_IMAGE_PLAN_PRODUCT_KEY);
    String publisher = customImagePlanCfgMap.get(CUSTOM_IMAGE_PLAN_PUBLISHER_KEY);

    if (name == null || product == null || publisher == null) {
      throw new ValidationException(String.format(invalidConfigErrMsg, customImagePlan));
    }

    PurchasePlan plan = new PurchasePlan()
        .withName(name)
        .withProduct(product)
        .withPublisher(publisher);
    return plan;
  }
}
