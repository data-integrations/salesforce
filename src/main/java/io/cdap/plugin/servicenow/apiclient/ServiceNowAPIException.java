package io.cdap.plugin.servicenow.apiclient;

import io.cdap.plugin.servicenow.util.ServiceNowConstants;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Custom Exception class for propagating API errors/exceptions back to caller.
 */
public class ServiceNowAPIException extends Exception {

  @Nullable private final HttpResponse httpResponse;
  @Nullable private final boolean manualRetry;

  private static final Set<Integer> RETRYABLE_CODES = new HashSet<>(Arrays.asList(429,
      HttpStatus.SC_BAD_GATEWAY,
      HttpStatus.SC_SERVICE_UNAVAILABLE,
      HttpStatus.SC_REQUEST_TIMEOUT,
      HttpStatus.SC_GATEWAY_TIMEOUT));

  public ServiceNowAPIException(String message, @Nullable HttpResponse httpResponse) {
    this(message, null, httpResponse, false);
  }

  public ServiceNowAPIException(Throwable t, @Nullable HttpResponse httpResponse) {
    this(null, t, httpResponse, false)
  }

  public ServiceNowAPIException(String message, Throwable t,
      @Nullable HttpResponse httpResponse, boolean manualRetry) {
    super(message, t);
    this.httpResponse = httpResponse;
    this.manualRetry = manualRetry;
  }

  public String getUnderlyingMessage() {
    if (this.getCause() != null) {
      return this.getCause().getMessage();
    }
    return null;
  }

  @Nullable
  public HttpResponse getHttpResponse() {
    return httpResponse;
  }

  public int getStatusCode() {
    if (httpResponse != null && httpResponse.getStatusLine() != null) {
      return httpResponse.getStatusLine().getStatusCode();
    }
    return 0;
  }

  public boolean isErrorRetryable() {
    if (manualRetry) {
      return true;
    }
    Throwable t = this.getCause();
    return t instanceof OAuthSystemException
        || (this.getMessage() != null
        && this.getMessage().contains(ServiceNowConstants.MAXIMUM_EXECUTION_TIME_EXCEEDED))
        || RETRYABLE_CODES.contains(getStatusCode());
  }
}
