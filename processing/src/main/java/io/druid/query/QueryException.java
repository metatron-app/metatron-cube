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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.druid.common.utils.ExceptionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.netty.channel.ChannelException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Fields:
 * - "errorCode" is a well-defined errorCode code taken from a specific list (see the static constants). "Unknown exception"
 * represents all wrapped exceptions other than interrupt/timeout/cancellation.
 * - "errorMessage" is the toString of the wrapped exception
 * - "errorClass" is the class of the wrapped exception
 * - "host" is the host that the errorCode occurred on
 * <p>
 * The QueryResource is expected to emit the JSON form of this object when errors happen, and the DirectDruidClient
 * deserializes and wraps them.
 */
public class QueryException extends RuntimeException
{
  public static enum Code
  {
    INTERRUPTED, TIMED_OUT, CANCELLED, RESOURCE_LIMIT_EXCEEDED, UNAUTHORIZED, UNSUPPORTED, UNKNOWN
  }

  private final Code errorCode;
  private final String errorClass;
  private final List<String> errorStack;

  private String host;
  private String serviceName;

  @JsonCreator
  public QueryException(
      @JsonProperty("errorCode") Code errorCode,
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
   * Creates a new QueryException wrapping an underlying exception. The errorMessage and errorClass
   * of this exception will be based on the highest non-QueryException in the causality chain.
   *
   * @param cause wrapped exception
   */
  public QueryException(Throwable cause)
  {
    this(cause, getHostFromThrowable(cause), getServiceNameFromThrowable(cause));
  }

  public QueryException(Throwable cause, String host, String serviceName)
  {
    super(cause == null ? null : cause.getMessage(), cause);
    this.errorCode = getErrorCodeFromThrowable(cause);
    this.errorClass = getErrorClassFromThrowable(cause);
    this.errorStack = cause == null ? null : ExceptionUtils.stackTrace(cause);
    this.host = host;
    this.serviceName = serviceName;
  }

  public static <T> T wrap(Callable<T> callable) throws QueryException
  {
    try {
      return callable.call();
    }
    catch (ExecutionException e) {
      throw wrapIfNeeded(e.getCause());
    }
    catch (Exception e) {
      throw wrapIfNeeded(e);
    }
  }

  @JsonProperty
  public Code getErrorCode()
  {
    return errorCode;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getErrorMessage()
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
  @JsonInclude(Include.NON_EMPTY)
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

  private static Code getErrorCodeFromThrowable(Throwable e)
  {
    if (e instanceof QueryException) {
      return ((QueryException) e).getErrorCode();
    } else if (e instanceof InterruptedException) {
      return Code.INTERRUPTED;
    } else if (e instanceof CancellationException) {
      return Code.CANCELLED;
    } else if (e instanceof TimeoutException || e instanceof org.jboss.netty.handler.timeout.TimeoutException) {
      return Code.TIMED_OUT;
    } else if (e instanceof RejectedExecutionException) {
      return Code.RESOURCE_LIMIT_EXCEEDED;
    } else {
      return Code.UNKNOWN;
    }
  }

  private static String getErrorClassFromThrowable(Throwable e)
  {
    if (e instanceof QueryException) {
      return ((QueryException) e).getErrorClass();
    } else if (e != null) {
      return e.getClass().getName();
    } else {
      return null;
    }
  }

  private static String getHostFromThrowable(Throwable e)
  {
    return e instanceof QueryException ? ((QueryException) e).getHost() : null;
  }

  private static String getServiceNameFromThrowable(Throwable e)
  {
    return e instanceof QueryException ? ((QueryException) e).getServiceName() : null;
  }

  public static QueryException wrapIfNeeded(Throwable e)
  {
    return wrapIfNeeded(e, null, null);
  }

  public static QueryException wrapIfNeeded(Throwable e, DruidNode node)
  {
    return wrapIfNeeded(e, node.getHostAndPort(), node.getServiceName());
  }

  public static QueryException wrapIfNeeded(Throwable e, String hostPort, String serviceName)
  {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof QueryException) {
        QueryException qie = (QueryException) t;
        if (qie.host == null && hostPort != null) {
          qie.host = hostPort;
        }
        if (qie.serviceName == null && serviceName != null) {
          qie.serviceName = serviceName;
        }
        return qie;
      }
      if (t instanceof TimeoutException ||
          t instanceof InterruptedException ||
          t instanceof org.jboss.netty.handler.timeout.TimeoutException ||
          t instanceof CancellationException ||
          t instanceof ChannelException ||
          t instanceof RejectedExecutionException) {
        // todo should retry to other replica if exists?
        e = t;
        break;
      }
    }
    return new QueryException(e, hostPort, serviceName);
  }

  public static QueryException read(byte[] contents, ObjectMapper mapper)
  {
    try {
      return contents.length == 0 ? null : mapper.readValue(contents, QueryException.class);
    }
    catch (Exception e) {
      return null;
    }
  }

  public static void warn(Logger log, Throwable t, String message, Object... params)
  {
    if (t instanceof QueryException) {
      log.warn(message, params);
      log.warn(((QueryException) t).detail());
    } else {
      log.warn(t, message, params);
    }
  }

  private String detail()
  {
    StringBuilder builder = new StringBuilder();
    if (host != null && serviceName != null) {
      builder.append('[').append(host).append(':').append(serviceName).append(']');
    }
    if (errorCode != null && errorCode != Code.UNKNOWN) {
      builder.append('[').append(errorCode).append(']');
    }
    if (builder.length() > 0) {
      builder.append(' ');
    }
    String message = getLocalizedMessage();
    if (errorClass != null && message != null) {
      builder.append(errorClass).append(": ").append(message);
    } else if (errorClass != null) {
      builder.append(errorClass);
    } else if (message != null) {
      builder.append(message);
    }
    for (String stack : Optional.fromNullable(errorStack).or(ImmutableList.of())) {
      builder.append("\n\tat ").append(stack);
    }
    return builder.toString();
  }
}
