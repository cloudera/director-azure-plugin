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

import com.cloudera.director.spi.v1.compute.util.AbstractComputeInstance;
import com.cloudera.director.spi.v1.model.DisplayProperty;
import com.cloudera.director.spi.v1.model.DisplayPropertyToken;
import com.cloudera.director.spi.v1.model.util.SimpleDisplayPropertyBuilder;
import com.cloudera.director.spi.v1.util.DisplayPropertiesUtil;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Azure compute instances.
 */
public class AzureComputeInstance extends AbstractComputeInstance<AzureComputeInstanceTemplate,
  AzureComputeInstanceHelper> {

  /**
   * The list of display properties (including inherited properties).
   */
  private static final List<DisplayProperty> DISPLAY_PROPERTIES =
    DisplayPropertiesUtil.asDisplayPropertyList(AzureComputeInstanceDisplayPropertyToken.values());
  private final AzureComputeInstanceHelper instanceHelper;

  public static List<DisplayProperty> getDisplayProperties() {
    return DISPLAY_PROPERTIES;
  }

  public enum AzureComputeInstanceDisplayPropertyToken implements DisplayPropertyToken {

    IMAGE_ID(new SimpleDisplayPropertyBuilder()
      .displayKey("imageId")
      .name("Image ID")
      .defaultDescription("The ID of the image used to launch the instance.")


      .build()) {
      @Override
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getImageReference();
      }
    },

    /**
     * The ID of the instance.
     */
    INSTANCE_ID(new SimpleDisplayPropertyBuilder()
      .displayKey("instanceId")
      .name("Instance ID")
      .defaultDescription("The ID of the instance.")
      .build()) {
      @Override
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getInstanceID();
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
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getInstanceType();
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
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getPrivateIpAddress().getHostAddress();
      }
    },

    /**
     * The private FQDN for each host
     */
    PRIVATE_FQDN(new SimpleDisplayPropertyBuilder()
      .displayKey("privateFqdn")
      .name("Private FQDN")
      .defaultDescription("The private FQDN for each host.")
      .build()) {
      @Override
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getPrivateFqdn();
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
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        InetAddress publicIp = instanceHelper.getPublicIpAddress();
        return (publicIp == null) ? null : publicIp.getHostAddress();
      }
    },

    /**
     * The public FQDN for each host
     */
    PUBLIC_FQDN(new SimpleDisplayPropertyBuilder()
      .displayKey("publicFqdn")
      .name("Public FQDN")
      .defaultDescription("The public FQDN for each host.")
      .build()) {
      @Override
      protected String getPropertyValue(AzureComputeInstanceHelper instanceHelper) {
        return instanceHelper.getPublicFqdn();
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
     * @param instanceHelper the wrapper around Azure compute instance
     * @return the value of the property from the specified instance
     */
    protected abstract String getPropertyValue(AzureComputeInstanceHelper instanceHelper);

    @Override
    public DisplayProperty unwrap() {
      return displayProperty;
    }
  }

  public static final Type TYPE = new ResourceType("AzureComputeInstance");


  public AzureComputeInstance(AzureComputeInstanceTemplate template, String instanceId, AzureComputeInstanceHelper
    instanceHelper) {
    super(template, instanceId, instanceHelper.getPrivateIpAddress(), null, instanceHelper);

    this.instanceHelper = instanceHelper;
  }

  @Override
  public InetAddress getPrivateIpAddress() {
    return instanceHelper.getPrivateIpAddress();
  }

  @Override
  public Type getType() {
    return TYPE;
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<String, String>();
    for (AzureComputeInstanceDisplayPropertyToken propertyToken : AzureComputeInstanceDisplayPropertyToken.values()) {
      properties.put(propertyToken.unwrap().getDisplayKey(), propertyToken.getPropertyValue(instanceHelper));
    }
    return properties;
  }
}
