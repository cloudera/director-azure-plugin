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

package com.cloudera.director.azure.compute.instance;

import com.cloudera.director.spi.v2.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v2.model.DisplayProperty;
import com.cloudera.director.spi.v2.model.DisplayPropertyToken;
import com.cloudera.director.spi.v2.model.util.SimpleDisplayPropertyBuilder;
import com.cloudera.director.spi.v2.util.DisplayPropertiesUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import com.microsoft.azure.management.compute.ImageReference;
import com.microsoft.azure.management.network.NetworkInterfaceBase;
import com.microsoft.azure.management.network.implementation.PublicIPAddressInner;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

/**
 * Azure compute instances.
 */
public class AzureComputeInstance<T extends AzureInstance>
    extends AbstractComputeInstance<AzureComputeInstanceTemplate, T> {

  /**
   * The list of display properties (including inherited properties).
   */
  private static final List<DisplayProperty> DISPLAY_PROPERTIES = DisplayPropertiesUtil
      .asDisplayPropertyList(AzureComputeInstanceDisplayPropertyToken.values());

  private static final Type TYPE = new ResourceType("AzureComputeInstance");

  public enum AzureComputeInstanceDisplayPropertyToken implements DisplayPropertyToken {

    /**
     * The ID of the image used to launch the instance.
     */
    IMAGE_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("imageId")
        .name("Image ID")
        .defaultDescription("The ID of the image used to launch the instance.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        ImageReference image = instance.storageProfile().imageReference();
        return String.format("Region: %s; Publisher: %s; Offer: %s; SKU: %s; Version: %s;",
            instance.regionName(), image.publisher(), image.sku(), image.offer(), image.version());
      }
    },

    /**
     * The ID of the instance along with supplemental information in the format:
     *   /subscriptions/<subscription UUID>/resourceGroups/<RG name>/providers
     *   /Microsoft.Compute/virtualMachines/<prefix>-<instanceIdUUID>
     */
    INSTANCE_ID(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceId")
        .name("Instance ID")
        .defaultDescription("The ID of the instance.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        return instance.name();
      }
    },

    /**
     * The instance type.
     */
    INSTANCE_TYPE(new SimpleDisplayPropertyBuilder()
        .displayKey("instanceType")
        .name("Machine type")
        .defaultDescription("The instance type.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        return instance.size().toString();
      }
    },

    /**
     * The private IP address assigned to the instance.
     */
    PRIVATE_IP_ADDRESS(new SimpleDisplayPropertyBuilder()
        .displayKey("privateIpAddress")
        .name("Internal IP")
        .defaultDescription("The private IP address assigned to the instance.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        NetworkInterfaceBase primaryNetworkInterface = instance.getPrimaryNetworkInterface();
        return primaryNetworkInterface == null ? null : primaryNetworkInterface.primaryPrivateIP();
      }
    },

    /**
     * The private FQDN for each host.
     */
    PRIVATE_FQDN(new SimpleDisplayPropertyBuilder()
        .displayKey("privateFqdn")
        .name("Private FQDN")
        .defaultDescription("The private FQDN for each host.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        return instance.computerName();
      }
    },

    /**
     * The public IP address assigned to the instance.
     */
    PUBLIC_IP_ADDRESS(new SimpleDisplayPropertyBuilder()
        .displayKey("publicIpAddress")
        .name("Public IP")
        .defaultDescription("The public IP address assigned to the instance.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        PublicIPAddressInner publicIp = instance.getPublicIPAddress();
        return publicIp == null ? null : publicIp.ipAddress();
      }
    },

    /**
     * The public FQDN for each host.
     */
    PUBLIC_FQDN(new SimpleDisplayPropertyBuilder()
        .displayKey("publicFqdn")
        .name("Public FQDN")
        .defaultDescription("The public FQDN for each host.")
        .build()) {

      @Override
      protected String getPropertyValue(@Nonnull AzureInstance instance) {
        PublicIPAddressInner publicIp = instance.getPublicIPAddress();
        return publicIp == null || publicIp.dnsSettings() == null ? null : publicIp.dnsSettings().fqdn();
      }
    };

    /**
     * The display property.
     */
    private final DisplayProperty displayProperty;

    /**
     * Creates a Azure instance display property token with the specified parameters.
     *
     * @param displayProperty the display property
     */
    AzureComputeInstanceDisplayPropertyToken(DisplayProperty displayProperty) {
      this.displayProperty = displayProperty;
    }

    /**
     * Returns the value of the property from the specified instance.
     *
     * @param instance the Azure VM
     * @return the value of the property from the specified instance
     */
    protected abstract String getPropertyValue(@Nonnull AzureInstance instance);

    @Override
    public DisplayProperty unwrap() {
      return displayProperty;
    }
  }

  /**
   * Creates an Azure compute instance with the specified parameters.
   *
   * @param template Azure compute instance template used to get user provided fields
   * @param instanceId the instance identifier
   * @param instanceDetails the provider-specific instance details used to populate all of the
   * fields in AzureComputeInstance
   * @throws IllegalArgumentException if the instance does not have a valid private IP
   */
  public AzureComputeInstance(
      AzureComputeInstanceTemplate template,
      String instanceId,
      T instanceDetails) {
    super(template, instanceId, getPrivateIpAddress(instanceDetails), null, instanceDetails);
  }

  @Override
  public InetAddress getPrivateIpAddress() {
    return getPrivateIpAddress(unwrap());
  }

  /**
   * Gets the list of display properties for an Azure instance, including inherited properties.
   *
   * @return the list of display properties
   */
  public static List<DisplayProperty> getDisplayProperties() {
    return DISPLAY_PROPERTIES;
  }

  @Override
  public Type getType() {
    return TYPE;
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();
    T instanceDetails = unwrap();
    for (AzureComputeInstanceDisplayPropertyToken propertyToken :
        AzureComputeInstanceDisplayPropertyToken.values()) {
      properties.put(
          propertyToken.unwrap().getDisplayKey(),
          instanceDetails == null ? null : propertyToken.getPropertyValue(instanceDetails));
    }
    return properties;
  }

  /**
   * Returns the private IP address of the specified Azure instance.
   *
   * @param instance the instance
   * @return the private IP address of the specified Azure instance; null if the VM or the VM's Nic are null, or if the
   * Nic's primary private IP is null or an empty string
   * @throws IllegalArgumentException if the private IP address is not null or empty string but is invalid
   */
  @VisibleForTesting
  static InetAddress getPrivateIpAddress(AzureInstance instance) {
    if (instance == null) {
      return null;
    }
    NetworkInterfaceBase nic = instance.getPrimaryNetworkInterface();
    if (nic == null) {
      return null;
    }
    String pip = nic.primaryPrivateIP();
    if (StringUtils.isEmpty(pip)) {
      return null;
    }

    return InetAddresses.forString(pip);
  }
}
