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
 * limitations under the License.
 */

package com.cloudera.director.azure;

import static com.google.common.base.Throwables.getRootCause;

import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.microsoft.aad.adal4j.AuthenticationException;
import com.microsoft.azure.CloudError;
import com.microsoft.azure.CloudException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides utilities for dealing with Azure exceptions.
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-common-deployment-errors">Azure Deployment Common Errors</a>
 *
 */
public class AzureExceptions {

  /**
   * Message provided to the corresponding error code.
   */
  protected static final String MESSAGE = "message";

  /**
   * Azure provider error code.
   */
  public static final String AZURE_ERROR_CODE = "azureErrorCode";

  /**
   * Azure provider error message.
   */
  protected static final String AZURE_ERROR_MESSAGE = "azureErrorMessage";

  /**
   * Image EULA not accepted error code.
   */
  public static final String IMAGE_EULA_NOT_ACCEPTED = "ResourcePurchaseValidationFailed";

  /**
   * Resource quota exceeded error code.
   */
  public static final String RESOURCE_QUOTA_EXCEEDED = "ResourceQuotaExceeded";

  /**
   * Authorization failed error code.
   */
  public static final String AUTHORIZATION_FAILED = "AuthorizationFailed";

  /**
   * Invalid authentication token error code.
   */
  public static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

  /**
   * Invalid client secret is provided.
   */
  static final String INVALID_CLIENT_SECRET = "InvalidClientSecret";
  private static final String INVALID_CLIENT_SECRET_AZURE_CODE = "AADSTS70002";

  /**
   * Tenant not found
   */
  static final String TENANT_NOT_FOUND = "TenantNotFound";
  private static final String TENANT_NOT_FOUND_AZURE_CODE = "AADSTS90002";

  /**
   * Operation not allowed error code.
   * Most likely because of exceeding quota.
   */
  public static final String OPERATION_NOT_ALLOWED = "OperationNotAllowed";

  /**
   * Public IP limit exceeded error code.
   */
  public static final String PUBLIC_IP_LIMIT_EXCEEDED = "PublicIPCountLimitReached";

  // The set of error codes representing authorization failures.
  private static final Set<String> AUTHORIZATION_ERROR_CODES = ImmutableSet.of(
      AUTHORIZATION_FAILED,
      INVALID_AUTHENTICATION_TOKEN,
      INVALID_CLIENT_SECRET,
      TENANT_NOT_FOUND
  );

  // The map of error code to message.
  private final static Map<String, String> AZURE_ERROR_CODE_TO_MESSAGE_MAP =
      ImmutableMap.<String, String>builder()
      .put(IMAGE_EULA_NOT_ACCEPTED, "Image EULA not accepted.")
      .put(RESOURCE_QUOTA_EXCEEDED, "Resource quota limit exceeded.")
      .put(AUTHORIZATION_FAILED, "Service principal doesn't have sufficient access.")
      .put(INVALID_AUTHENTICATION_TOKEN, "Service principal is not assigned.")
      .put(INVALID_CLIENT_SECRET, "Invalid client secret is provided.")
      .put(TENANT_NOT_FOUND, "Tenant not found.")
      .put(OPERATION_NOT_ALLOWED, "Operation is not allowed.")
      .put(PUBLIC_IP_LIMIT_EXCEEDED, "Public IP limit exceeded.")
      .build();

  private static final String DEFAULT_MESSAGE = "Encountered Azure exception.";

  /**
   * Parses an exception and throws an appropriate plugin exception with appropriate
   * plugin exception details.
   * This will throw an {@link InvalidCredentialsException} if the error code is one of the
   * authorization errors, otherwise throw an {@link UnrecoverableProviderException}.
   *
   * @param message the plugin exception message to set
   * @param e       the exception
   * @return ignore return value
   */
  public static AbstractPluginException propagateUnrecoverable(String message, Exception e) {
    Set<Exception> exceptions = Sets.newHashSet();
    collectExceptions(exceptions, e);
    throw propagateUnrecoverable(message, exceptions);
  }

  private static void collectExceptions(Set<Exception> exceptions, Exception e) {
    if (e == null) {
      return;
    }
    exceptions.add(e);
    Throwable[] suppressed = e.getSuppressed();
    for (Throwable t : suppressed) {
      Throwables.throwIfInstanceOf(t, Error.class);
      collectExceptions(exceptions, (Exception) t);
    }
  }

  /**
   * Parses a set of exceptions and throws an appropriate plugin exception with
   * appropriate plugin exception details.
   * This will throw an {@link InvalidCredentialsException} if one of the error codes
   * contain authorization error, otherwise throw an {@link UnrecoverableProviderException}.
   *
   * @param message    the plugin exception message to set
   * @param exceptions a set of exceptions
   * @return ignore return value
   */
  public static AbstractPluginException propagateUnrecoverable(String message, Set<Exception> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      throw new UnrecoverableProviderException(message);
    }

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    Set<String> azureErrorCodes = addErrors(exceptions, accumulator);

    PluginExceptionDetails pluginExceptionDetails =
        new PluginExceptionDetails(accumulator.getConditionsByKey());

    if (azureErrorCodes.stream().anyMatch(AUTHORIZATION_ERROR_CODES::contains)) {
      throw new InvalidCredentialsException(message, pluginExceptionDetails);
    }
    throw new UnrecoverableProviderException(message, pluginExceptionDetails);
  }

  /**
   * Converts a set of exceptions to plugin error conditions. These conditions
   * will be added to the provided accumulator.
   *
   * @param exceptions  the set of encountered exceptions
   * @param accumulator the exception condition accumulator to add to
   * @return the set of azure error code
   */
  private static Set<String> addErrors(Set<Exception> exceptions,
                                   PluginExceptionConditionAccumulator accumulator) {
    // Only report each error code once
    Map<String, CloudError> azureErrors = Maps.newHashMap();
    for (Exception e : exceptions) {
      if (e instanceof CloudException) {
        CloudError ce = ((CloudException) e).body();
        if (ce == null) continue;
        // CloudError may contain a list of more detailed CloudErrors.
        // If so, only add errors from details since the top level error is too generic.
        // Note that the cloudError from details might also contain details.
        // However, this is presumably very rare. Need to fix if it turned out not to be the case.
        List<CloudError> details = ce.details();
        if (details == null || details.isEmpty()) {
          azureErrors.put(ce.code(), ce);
        } else {
          details.forEach(error -> {
            if (error != null) {
              azureErrors.put(error.code(), error);
            }
          });
        }
      } else if (getRootCause(e) instanceof AuthenticationException) {
        if (e.getMessage().contains(INVALID_CLIENT_SECRET_AZURE_CODE)) {
          azureErrors.put(INVALID_CLIENT_SECRET, null);
          accumulator.addError(ImmutableMap.of(
              MESSAGE, "Error validating credentials. Invalid client secret is provided.",
              AZURE_ERROR_CODE, INVALID_CLIENT_SECRET,
              AZURE_ERROR_MESSAGE, e.getMessage()));
        } else if (e.getMessage().contains(TENANT_NOT_FOUND_AZURE_CODE)) {
          azureErrors.put(TENANT_NOT_FOUND, null);
          accumulator.addError(ImmutableMap.of(
              MESSAGE, "Error validating credentials. Tenant not found.",
              AZURE_ERROR_CODE, TENANT_NOT_FOUND,
              AZURE_ERROR_MESSAGE, e.getMessage()));
        }
      } else {
        accumulator.addError(toExceptionInfoMap(e.getMessage(), "N/A", "N/A"));
      }
    }

    for (CloudError azureError : azureErrors.values()) {
      if (azureError != null) {
        accumulator.addError(toExceptionInfoMap(azureError));
      }
    }
    return azureErrors.keySet();
  }

  private static Map<String, String> toExceptionInfoMap(CloudError ce) {
    String azureErrorCode = ce.code();
    String message = AZURE_ERROR_CODE_TO_MESSAGE_MAP.getOrDefault(azureErrorCode, DEFAULT_MESSAGE);
    return toExceptionInfoMap(message, azureErrorCode, ce.message());
  }

  private static Map<String, String> toExceptionInfoMap(String message,
                                                        String azureErrorCode,
                                                        String azureErrorMessage) {
    return ImmutableMap.of(
        MESSAGE, message,
        AZURE_ERROR_CODE, azureErrorCode,
        AZURE_ERROR_MESSAGE, azureErrorMessage
    );
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private AzureExceptions() {
  }
}
