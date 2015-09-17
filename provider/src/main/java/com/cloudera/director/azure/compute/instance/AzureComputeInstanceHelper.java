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

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.microsoft.azure.management.compute.models.NetworkInterfaceReference;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.PublicIpAddress;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to hide the details of interacting with Azure SDK.
 */
public class AzureComputeInstanceHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeInstanceHelper.class);
  private static final boolean PUBLIC = true;
  private static final boolean PRIVATE = false;

  private final String resourceGroupName;
  private final AzureCredentials cred;
  private final VirtualMachine vm;
  private final InetAddress publicIP; // can be null if public IP is not configured
  private final InetAddress privateIP;

  public AzureComputeInstanceHelper(VirtualMachine vm, AzureCredentials cred, String resourceGroupName)
    throws IOException, ServiceException {
    this.cred = cred;
    this.vm = vm;
    this.resourceGroupName = resourceGroupName;
    this.publicIP = getIpAddress(PUBLIC);
    this.privateIP = getIpAddress(PRIVATE);
  }

  public String getImageReference() {
    LOG.debug("Get image reference for VM {}.", vm.getName());
    return vm.getStorageProfile().getImageReference().getOffer();
  }

  public String getInstanceID() {
    LOG.debug("Get instance ID for VM {}.", vm.getName());
    return vm.getId();
  }

  public String getInstanceType() {
    LOG.debug("Get instance type for VM {}.", vm.getName());
    return vm.getHardwareProfile().getVirtualMachineSize();
  }

  public InetAddress getPrivateIpAddress() {
    if (privateIP == null) {
      LOG.error("Private IP address for VM {} is null.", vm.getName());
      throw new IllegalArgumentException("Private IP address for VM " + vm.getName() + " is null.");
    }
    LOG.debug("Get private IP address for VM {}.", vm.getName());
    return privateIP;
  }

  public InetAddress getPublicIpAddress() {
    LOG.debug("Get public IP address for VM {}.", vm.getName());
    return publicIP;
  }

  // AZURE_SDK Azure SDK does not provide an easy way to retrieve IP addresses.
  private InetAddress getIpAddress(boolean isPublic) throws IOException, ServiceException {
    Configuration config = cred.createConfiguration();
    NetworkResourceProviderClient networkResourceProviderClient = NetworkResourceProviderService.create(config);
    ArrayList<NetworkInterfaceReference> nics = vm.getNetworkProfile().getNetworkInterfaces();
    //CDH deployment on Azure with one nic per VM
    NetworkInterfaceReference nicReference = nics.get(0);
    String[] nicURI = nicReference.getReferenceUri().split("/");

    NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(
      resourceGroupName, nicURI[nicURI.length - 1]).getNetworkInterface();
    LOG.debug("NIC = {}.", nic.getName());
    ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();
    NetworkInterfaceIpConfiguration ipConfig = ips.get(0);
    LOG.debug("ipConfig = {}.", ipConfig.getName());
    if (isPublic) {
      if (ipConfig.getPublicIpAddress() != null) {
        String[] pipID = ipConfig.getPublicIpAddress().getId().split("/");
        LOG.debug("pipID = {}.", Arrays.toString(pipID));
        PublicIpAddress pip = networkResourceProviderClient.getPublicIpAddressesOperations()
          .get(resourceGroupName, pipID[pipID.length - 1]).getPublicIpAddress();
        return InetAddress.getByName(pip.getIpAddress());
      } else {
        LOG.info("Public IP is not configured for VM {}.", vm.getName());
        return null;
      }
    } else {
      return InetAddress.getByName(ipConfig.getPrivateIpAddress());
    }
  }
}
