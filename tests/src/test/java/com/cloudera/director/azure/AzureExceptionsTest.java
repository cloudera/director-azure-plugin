// (c) Copyright 2018 Cloudera, Inc.

package com.cloudera.director.azure;

import static com.cloudera.director.azure.AzureExceptions.AZURE_ERROR_CODE;
import static com.cloudera.director.azure.AzureExceptions.AZURE_ERROR_MESSAGE;
import static com.cloudera.director.azure.AzureExceptions.MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.cloudera.director.spi.v2.model.exception.AbstractPluginException;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionCondition;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.cloudera.director.azure.shaded.com.microsoft.azure.CloudError;
import com.cloudera.director.azure.shaded.com.microsoft.azure.CloudException;
import org.junit.Test;

public class AzureExceptionsTest {

  private static final String PLUGIN_ERROR_MESSAGE = "Plugin error message";

  private static final String AZURE_PROVIDER_ERROR_MESSAGE = "Azure provider error message";

  private static Exception createException(String errorMessage) {
    return new Exception(errorMessage);
  }

  private static CloudException createCloudException(String errorCode) {
    return new CloudException("Test Azure exception",
        null,
        new CloudError()
            .withCode(errorCode)
            .withMessage(AZURE_PROVIDER_ERROR_MESSAGE));
  }

  @Test(expected = InvalidCredentialsException.class)
  public void testAuthorizationFailedException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.AUTHORIZATION_FAILED));
    } catch (InvalidCredentialsException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Service principal doesn't have sufficient access.",
          AzureExceptions.AUTHORIZATION_FAILED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = InvalidCredentialsException.class)
  public void testInvalidAuthenticationTokenException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.INVALID_AUTHENTICATION_TOKEN));
    } catch (InvalidCredentialsException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Service principal is not assigned.",
          AzureExceptions.INVALID_AUTHENTICATION_TOKEN,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testInvalidClientSecretException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.INVALID_CLIENT_SECRET));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Invalid client secret is provided.",
          AzureExceptions.INVALID_CLIENT_SECRET,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testTenantNotFoundException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.TENANT_NOT_FOUND));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Tenant not found.",
          AzureExceptions.TENANT_NOT_FOUND,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testResourcePurchaseValidationFailedException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.IMAGE_EULA_NOT_ACCEPTED));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Image EULA not accepted.",
          AzureExceptions.IMAGE_EULA_NOT_ACCEPTED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testResourceQuotaExceededException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.RESOURCE_QUOTA_EXCEEDED));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Resource quota limit exceeded.",
          AzureExceptions.RESOURCE_QUOTA_EXCEEDED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testOperationNotAllowedException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.OPERATION_NOT_ALLOWED));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Operation is not allowed.",
          AzureExceptions.OPERATION_NOT_ALLOWED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testPublicIPCountLimitReachedException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createCloudException(AzureExceptions.PUBLIC_IP_LIMIT_EXCEEDED));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Public IP limit exceeded.",
          AzureExceptions.PUBLIC_IP_LIMIT_EXCEEDED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testNonCloudExceptionWithDetails() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE,
          createException("error message"));
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "error message",
          "N/A",
          "N/A");
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testNestedCloudExceptionWithDetails() {
    CloudException e = createCloudException("ResourceDeploymentFailure");
    e.body().details().add(new CloudError()
        .withCode(AzureExceptions.RESOURCE_QUOTA_EXCEEDED)
        .withMessage(AZURE_PROVIDER_ERROR_MESSAGE));
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE, e);
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifySingleError(ex,
          "Resource quota limit exceeded.",
          AzureExceptions.RESOURCE_QUOTA_EXCEEDED,
          AZURE_PROVIDER_ERROR_MESSAGE);
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testSuppressedExceptionWithDetails() {
    Exception e = createCloudException(AzureExceptions.RESOURCE_QUOTA_EXCEEDED);
    e.addSuppressed(createException("error message"));
    e.getSuppressed()[0].addSuppressed(createCloudException(AzureExceptions.IMAGE_EULA_NOT_ACCEPTED));
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE, e);
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      verifyMultipleErrorCodes(ex,
          ImmutableSet.of(
              AzureExceptions.RESOURCE_QUOTA_EXCEEDED,
              AzureExceptions.IMAGE_EULA_NOT_ACCEPTED,
              "N/A")
      );
      throw ex;
    }
  }

  @Test(expected = UnrecoverableProviderException.class)
  public void testNullException() {
    try {
      throw AzureExceptions.propagateUnrecoverable(PLUGIN_ERROR_MESSAGE, (Exception) null);
    } catch (UnrecoverableProviderException ex) {
      assertEquals(PLUGIN_ERROR_MESSAGE, ex.getMessage());
      assertEquals(PluginExceptionDetails.DEFAULT_DETAILS, ex.getDetails());
      throw ex;
    }
  }

  private void verifySingleError(AbstractPluginException ex, String expectedMessage,
                                     String expectedErrorCode, String expectedErrorMessage) {
    Map<String, SortedSet<PluginExceptionCondition>> conditionsByKey =
        ex.getDetails().getConditionsByKey();
    SortedSet<PluginExceptionCondition> conditions = conditionsByKey.get(null);
    PluginExceptionCondition condition = Iterables.getOnlyElement(conditions);
    Map<String, String> exceptionInfo = condition.getExceptionInfo();
    assertEquals(expectedMessage, exceptionInfo.get(MESSAGE));
    assertEquals(expectedErrorCode, exceptionInfo.get(AZURE_ERROR_CODE));
    assertEquals(expectedErrorMessage, exceptionInfo.get(AZURE_ERROR_MESSAGE));
  }

  private void verifyMultipleErrorCodes(AbstractPluginException ex, Set<String> expectedErrorCodes) {
    Map<String, SortedSet<PluginExceptionCondition>> conditionsByKey =
        ex.getDetails().getConditionsByKey();
    SortedSet<PluginExceptionCondition> conditions = conditionsByKey.get(null);
    assertEquals(expectedErrorCodes.size(), conditions.size());
    conditions.stream().forEach(condition ->
      assertTrue(expectedErrorCodes.contains(condition.getExceptionInfo().get(AZURE_ERROR_CODE))));
  }
}
