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
 * Azure VM Image info. Contains the information that uniquely identifies a VM image in Azure.
 */
public class AzureVmImageInfo {
  private String publisher;
  private String sku;
  private String offer;
  private String version;
  private String toStringFormat = "PUBLISHER: '%s', OFFER: '%s', SKU: '%s', VERSION: '%s'";

  public String getSku() {
    return sku;
  }

  public String getOffer() {
    return offer;
  }

  public String getVersion() {
    return version;
  }

  public String getPublisher() {
    return publisher;
  }

  public AzureVmImageInfo(String publisher, String sku, String offer, String version) {
    this.publisher = publisher;
    this.sku = sku;
    this.offer = offer;
    this.version = version;
  }

  @Override
  public String toString() {
    return String.format(toStringFormat, publisher, offer, sku, version);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AzureVmImageInfo))
      return false;
    if (obj == this)
      return true;

    AzureVmImageInfo rhs = (AzureVmImageInfo) obj;
    return this.toString().equals(rhs.toString());
  }

}
