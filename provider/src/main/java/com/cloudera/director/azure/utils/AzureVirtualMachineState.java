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

package com.cloudera.director.azure.utils;

/**
 * Azure VM power states.
 */
public class AzureVirtualMachineState {
  // AZURE_SDK Azure SDK does not provide pre-defined strings for VM power state.
  // FIXME find out all possible VM states and map them here
  public static final String POWER_STATE_RUNNING = "PowerState/running";
  public static final String POWER_STATE_DEALLOCATED = "PowerState/deallocated";

  public static final String PROVISIONING_STATE_SUCCEEDED = "ProvisioningState/succeeded";
  public static final String PROVISIONING_STATE_DELETING = "ProvisioningState/deleting";
  public static final String PROVISIONING_STATE_FAILED = "ProvisioningState/failed";
}
