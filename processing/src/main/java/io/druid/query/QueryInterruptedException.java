/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.ResourceLimitExceededException;
import io.druid.common.utils.ExceptionUtils;
import io.druid.server.DruidNode;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Exception representing a failed query. The name "QueryInterruptedException" is a misnomer; this is actually
 * used on the client side for *all* kinds of failed queries.
 *
 * Fields:
 * - "errorCode" is a well-defined errorCode code taken from a specific list (see the static constants). "Unknown exception"
 * represents all wrapped exceptions other than interrupt/timeout/cancellation.
 * - "errorMessage" is the toString of the wrapped exception
 * - "errorClass" is the class of the wrapped exception
 * - "host" is the host that the errorCode occurred on
 *
 * The QueryResource is expected to emit the JSON form of this object when errors happen, and the DirectDruidClient
 * deserializes and wraps them.
 */
public class QueryInterruptedException extends RuntimeException
{
  public static final String QUERY_INTERRUPTED = "Query interrupted";
  public static final String QUERY_TIMEOUT = "Query timeout";
  public static final String QUERY_CANCELLED = "Query cancelled";
  public static final String RESOURCE_LIMIT_EXCEEDED = "Resource limit exceeded";
  public static final String UNAUTHORIZED = "Unauthorized request.";
  public static final String UNSUPPORTED_OPERATION = "Unsupported operation";
  public static final String UNKNOWN_EXCEPTION = "Unknown exception";

  private final String errorCode;
  private final String errorClass;
  private final List<String> errorStack;
  private String host;
  private String serviceName;

  @JsonCreator
  public QueryInterruptedException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass,
      @JsonProperty("errorStack") List<String> errorStack,
      @JsonProperty("host") String host,
      @JsonProperty("serviceName") String serviceName
  )
  {
    super(errorMessage);
    this.errorCode = errorCode;
    this.errorClass = errorClass;
    this.errorStack = errorStack;
    this.host = host;
    this.serviceName = serviceName;
  }

  /**
   * Creates a new QueryInterruptedException wrapping an underlying exception. The errorMessage and errorClass
   * of this exception will be based on the highest non-QueryInterruptedException in the causality chain.
   *
   * @param cause wrapped exception
   */
  public QueryInterruptedException(Throwable cause)
  {
    this(cause, getHostFromThrowable(cause), getServiceNameFromThrowable(cause));
  }

  public QueryInterruptedException(Throwable cause, String host, String serviceName)
  {
    super(cause == null ? null : cause.getMessage(), cause);
    this.errorCode = getErrorCodeFromThrowable(cause);
    this.errorClass = getErrorClassFromThrowable(cause);
    this.errorStack = cause == null ? null : stackTrace(cause);
    this.host = host;
    this.serviceName = serviceName;
  }

  @JsonProperty("error")
  public String getErrorCode()
  {
    return errorCode;
  }

  @JsonProperty("errorMessage")
  @Override
  public String getMessage()
  {
    return super.getMessage();
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getErrorClass()
  {
    return errorClass;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public List<String> getErrorStack()
  {
    return errorStack;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getServiceName()
  {
    return serviceName;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setServiceName(String serviceName)
  {
    this.serviceName = serviceName;
  }

  private static String getErrorCodeFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getErrorCode();
    } else if (e instanceof InterruptedException) {
      return QUERY_INTERRUPTED;
    } else if (e instanceof CancellationException) {
      return QUERY_CANCELLED;
    } else if (e instanceof TimeoutException || e instanceof org.jboss.netty.handler.timeout.TimeoutException) {
      return QUERY_TIMEOUT;
    } else if (e instanceof ResourceLimitExceededException || e instanceof RejectedExecutionException) {
      return RESOURCE_LIMIT_EXCEEDED;
    } else {
      return UNKNOWN_EXCEPTION;
    }
  }

  private static String getErrorClassFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getErrorClass();
    } else if (e != null) {
      return e.getClass().getName();
    } else {
      return null;
    }
  }

  private static String getHostFromThrowable(Throwable e)
  {
    return e instanceof QueryInterruptedException ? ((QueryInterruptedException) e).getHost() : null;
  }

  private static String getServiceNameFromThrowable(Throwable e)
  {
    return e instanceof QueryInterruptedException ? ((QueryInterruptedException) e).getServiceName() : null;
  }

  public static QueryInterruptedException wrapIfNeeded(Throwable e)
  {
    return wrapIfNeeded(e, getHostFromThrowable(e), getServiceNameFromThrowable(e));
  }

  public static QueryInterruptedException wrapIfNeeded(Throwable e, DruidNode node)
  {
    return wrapIfNeeded(e, node.getHostAndPort(), node.getServiceName());
  }

  public static QueryInterruptedException wrapIfNeeded(Throwable e, String hostPort, String serviceName)
  {
    if (e instanceof QueryInterruptedException) {
      QueryInterruptedException qie = (QueryInterruptedException) e;
      if (qie.getHost() == null && hostPort != null) {
        qie.setHost(hostPort);
      }
      if (qie.getServiceName() == null && serviceName != null) {
        qie.setServiceName(serviceName);
      }
      return qie;
    } else {
      return new QueryInterruptedException(e, hostPort, serviceName);
    }
  }

  public static List<String> stackTrace(Throwable e)
  {
    return ExceptionUtils.stackTrace(e, Sets.<Throwable>newHashSet(), Lists.<String>newArrayList(), "");
  }

}
