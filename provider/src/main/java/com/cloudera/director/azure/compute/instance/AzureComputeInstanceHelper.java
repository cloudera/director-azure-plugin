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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;

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
  private final InetAddress privateIP;
  private final String privateFqdn;
  private final InetAddress publicIP; // can be null if public IP is not configured
  private final String publicFqdn; // can be null if public IP is not configured

  public AzureComputeInstanceHelper(VirtualMachine vm, AzureCredentials cred,
    String resourceGroupName)
    throws IOException, ServiceException {
    this.cred = cred;
    this.vm = vm;
    this.resourceGroupName = resourceGroupName;
    this.privateIP = getIpAddress(PRIVATE);
    this.privateFqdn = getFqdn(PRIVATE);
    this.publicIP = getIpAddress(PUBLIC);
    this.publicFqdn = getFqdn(PUBLIC);
  }

  public String getVMName() {
    LOG.debug("Get VM name for VM {}.", vm.getName());
    return vm.getName();
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
    LOG.debug("Get private IP {} address for VM {}.", privateIP, vm.getName());
    return privateIP;
  }

  public String getPrivateFqdn() {
    if (privateFqdn == null) {
      LOG.error("Private FQDN for VM {} is null.", vm.getName());
    }
    LOG.debug("Get private FQDN {} for VM {}.", privateFqdn, vm.getName());
    return privateFqdn;
  }

  public InetAddress getPublicIpAddress() {
    LOG.debug("Get public IP address {} for VM {}.", publicIP, vm.getName());
    return publicIP;
  }

  public String getPublicFqdn() {
    LOG.debug("Get public FQDN {} for VM {}.", publicFqdn, vm.getName());
    return publicFqdn;
  }

  private NetworkInterface getNetworkInterface(
    NetworkResourceProviderClient networkResourceProviderClient)
    throws IOException, ServiceException {
    ArrayList<NetworkInterfaceReference> nics = vm.getNetworkProfile().getNetworkInterfaces();
    // Azure plugin assumes there is only 1 NIC per VM.
    NetworkInterfaceReference nicReference = nics.get(0);
    String[] nicURI = nicReference.getReferenceUri().split("/");
    NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(
      resourceGroupName, nicURI[nicURI.length - 1]).getNetworkInterface();
    LOG.debug("NIC: {}.", nic.getName());
    return nic;
  }

  private NetworkInterfaceIpConfiguration getIpConfig(
    NetworkResourceProviderClient networkResourceProviderClient)
    throws IOException, ServiceException {
    NetworkInterface nic = getNetworkInterface(networkResourceProviderClient);
    ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();
    NetworkInterfaceIpConfiguration ipConfig = ips.get(0);
    LOG.debug("ipConfig: {}.", ipConfig.getName());
    return ipConfig;
  }

  private PublicIpAddress getPublicIP(NetworkInterfaceIpConfiguration ipConfig,
    NetworkResourceProviderClient networkResourceProviderClient)
    throws IOException, ServiceException {
    String[] pipID = ipConfig.getPublicIpAddress().getId().split("/");
    LOG.debug("pipID: {}.", Arrays.toString(pipID));
    return networkResourceProviderClient.getPublicIpAddressesOperations()
      .get(resourceGroupName, pipID[pipID.length - 1]).getPublicIpAddress();
  }

  private String getFqdn(boolean isPublic) throws IOException, ServiceException {
    Configuration config = cred.createConfiguration();
    NetworkResourceProviderClient networkResourceProviderClient = NetworkResourceProviderService
      .create(config);

    if (isPublic) {
      NetworkInterfaceIpConfiguration ipConfig = getIpConfig(networkResourceProviderClient);
      if (ipConfig.getPublicIpAddress() != null) {
        try {
          return getPublicIP(ipConfig, networkResourceProviderClient).getDnsSettings().getFqdn();
        } catch (ServiceException e) {
          LOG.error("Public IP for instance {} not found due to error: {}", getInstanceID(),
              e.getMessage());
          return null;
        }
      } else {
        LOG.info("Public IP is not configured for VM {}.", vm.getName());
        return null;
      }
    } else {
      // AZURE_SDK `nic.getDnsSettings().getInternalFqdn()` returns null
      // OSProfile can be null in certain rare cases
      return (vm.getOSProfile() == null) ? null : vm.getOSProfile().getComputerName();
    }
  }

  // AZURE_SDK Azure SDK does not provide an easy way to retrieve IP addresses.
  private InetAddress getIpAddress(boolean isPublic) throws IOException, ServiceException {
    Configuration config = cred.createConfiguration();
    NetworkResourceProviderClient networkResourceProviderClient = NetworkResourceProviderService
      .create(config);
    NetworkInterfaceIpConfiguration ipConfig = getIpConfig(networkResourceProviderClient);

    if (isPublic) {
      if (ipConfig.getPublicIpAddress() != null) {
        try {
          String ipAddress = getPublicIP(ipConfig, networkResourceProviderClient).getIpAddress();
          // When VM is de-allocated, public IP Address resource will be present but the IP address
          // be null. InetAddress.getByName() will interpret null address to loopback (127.0.0.1).
          return (ipAddress == null) ? null : InetAddress.getByName(ipAddress);
        } catch (ServiceException e) {
          LOG.error("Public IP for instance {} not found due to error: {}", getInstanceID(),
              e.getMessage());
          return null;
        }
      } else {
        LOG.info("Public IP is not configured for VM {}.", vm.getName());
        return null;
      }
    } else {
      return InetAddress.getByName(ipConfig.getPrivateIpAddress());
    }
  }

  /**
   * Test only: Gets the private IP allocation method string
   *
   * @return the private IP allocation method (static or dynamic)
   * @throws IOException      when there's an IO (networking) error
   * @throws ServiceException when Azure backend returns an error
   */
  public String getPrivateIpAllocationMethod() throws IOException, ServiceException {
    LOG.debug("Get private IP allocation method for VM {}.", vm.getName());
    Configuration config = cred.createConfiguration();
    NetworkResourceProviderClient networkResourceProviderClient = NetworkResourceProviderService
        .create(config);
    NetworkInterfaceIpConfiguration ipConfig = getIpConfig(networkResourceProviderClient);
    LOG.debug("Private IP allocation method is: {}.", ipConfig.getPrivateIpAllocationMethod());
    return ipConfig.getPrivateIpAllocationMethod();
  }
}
