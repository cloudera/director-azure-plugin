/*
 * Copyright (c) 2018 Cloudera, Inc.
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
 *  limitations under the License.
 *
 */

package com.cloudera.director.azure.compute.provider;

import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureInstance;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.microsoft.azure.management.compute.InstanceViewStatus;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for instance manipulation.
 */
public interface InstanceAllocator {

  /**
   * Allocates instances according to specified parameters.
   *
   * @param localizationContext localization context of provider
   * @param template            instance template
   * @param instanceIds         instance Ids
   * @param minCount            min instance count which needs to be allocated
   * @return a collection of successfully allocated instances if allocation succeeds. Otherwise, it should return an
   * empty collection
   */
  Collection<? extends AzureComputeInstance<? extends AzureInstance>> allocate(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds,
      int minCount)
      throws InterruptedException;

  /**
   * Describes a list of instances.
   *
   * @param localizationContext localization context of provider
   * @param template            instance template
   * @param instanceIds         instance Ids
   * @return a list of instances retrieved with specified parameters
   */
  Collection<? extends AzureComputeInstance<? extends AzureInstance>> find(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException;

  /**
   * Describes a list instance status.
   *
   * @param localizationContext localization context of provider
   * @param template            instance template
   * @param instanceIds         instance Ids
   * @return a map of instanceId to status list for the specified instances
   */
  Map<String, List<InstanceViewStatus>> getInstanceState(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds);

  /**
   * Deletes the specified instances.
   *
   * @param localizationContext localization context of provider
   * @param template            instance template
   * @param instanceIds         instance Ids
   */
  void delete(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException;

  /**
   * Gets the host keys from the specified instances.
   * @param localizationContext localization context of provider
   * @param template            instance template
   * @param instanceIds         instance Ids
   * @return a map of instanceId to host key fingerprints for the specified instances
   */
  Map<String, Set<String>> getHostKeyFingerprints(
      LocalizationContext localizationContext,
      AzureComputeInstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException;
}
