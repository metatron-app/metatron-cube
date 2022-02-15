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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.query.QueryException.Code;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

public class QueryExceptionTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testErrorCode()
  {
    Assert.assertEquals(
        Code.CANCELLED,
        new QueryException(new QueryException(new CancellationException())).getErrorCode()
    );
    Assert.assertEquals(Code.CANCELLED, new QueryException(new CancellationException()).getErrorCode());
    Assert.assertEquals(Code.INTERRUPTED, new QueryException(new InterruptedException()).getErrorCode());
    Assert.assertEquals(Code.TIMED_OUT, new QueryException(new TimeoutException()).getErrorCode());
    Assert.assertEquals(Code.UNKNOWN, new QueryException(null).getErrorCode());
    Assert.assertEquals(Code.UNKNOWN, new QueryException(new ISE("Something bad!")).getErrorCode());
    Assert.assertEquals(Code.UNKNOWN, new QueryException(new QueryException(new ISE("Something bad!"))).getErrorCode());
  }

  @Test
  public void testErrorMessage()
  {
    Assert.assertNull(new QueryException(new QueryException(new CancellationException())).getMessage());
    Assert.assertNull(new QueryException(new CancellationException()).getMessage());
    Assert.assertNull(new QueryException(new InterruptedException()).getMessage());
    Assert.assertNull(new QueryException(new TimeoutException()).getMessage());
    Assert.assertNull(new QueryException(null).getMessage());
    Assert.assertEquals(
        "Something bad!",
        new QueryException(new ISE("Something bad!")).getMessage()
    );
    Assert.assertEquals(
        "Something bad!",
        new QueryException(new QueryException(new ISE("Something bad!"))).getMessage()
    );
  }

  @Test
  public void testErrorClass()
  {
    Assert.assertEquals(
        "java.util.concurrent.CancellationException",
        new QueryException(new QueryException(new CancellationException())).getErrorClass()
    );
    Assert.assertEquals(
        "java.util.concurrent.CancellationException",
        new QueryException(new CancellationException()).getErrorClass()
    );
    Assert.assertEquals(
        "java.lang.InterruptedException",
        new QueryException(new InterruptedException()).getErrorClass()
    );
    Assert.assertEquals(
        "java.util.concurrent.TimeoutException",
        new QueryException(new TimeoutException()).getErrorClass()
    );
    Assert.assertNull(new QueryException(null).getErrorClass());
    Assert.assertEquals(
        "io.druid.java.util.common.ISE",
        new QueryException(new ISE("Something bad!")).getErrorClass()
    );
    Assert.assertEquals(
        "io.druid.java.util.common.ISE",
        new QueryException(new QueryException(new ISE("Something bad!"))).getErrorClass()
    );
  }

  @Test
  public void testHost()
  {
    QueryException cause = new QueryException(new QueryException(new CancellationException(), "myhost", "service"));
    Assert.assertEquals("myhost", cause.getHost());
    Assert.assertEquals("service", cause.getServiceName());

    cause = QueryException.wrapIfNeeded(new QueryException(new CancellationException()), "myhost", "service");
    Assert.assertEquals("myhost", cause.getHost());
    Assert.assertEquals("service", cause.getServiceName());
  }

  @Test
  public void testSerde()
  {
    QueryException qie = roundTrip(new QueryException(new QueryException(new CancellationException())));
    Assert.assertEquals(Code.CANCELLED, qie.getErrorCode());
    Assert.assertEquals("java.util.concurrent.CancellationException", qie.getErrorClass());
    Assert.assertNull(qie.getMessage());

    qie = roundTrip(new QueryException(new CancellationException()));
    Assert.assertEquals("java.util.concurrent.CancellationException", qie.getErrorClass());

    qie = roundTrip(new QueryException(new InterruptedException()));
    Assert.assertEquals("java.lang.InterruptedException", qie.getErrorClass());

    qie = roundTrip(new QueryException(new TimeoutException()));
    Assert.assertEquals("java.util.concurrent.TimeoutException", qie.getErrorClass());

    qie = roundTrip(new QueryException(null));
    Assert.assertNull(qie.getErrorClass());

    qie = roundTrip(new QueryException(new ISE("Something bad!")));
    Assert.assertEquals("io.druid.java.util.common.ISE", qie.getErrorClass());
    Assert.assertEquals("Something bad!", qie.getMessage());
    Assert.assertEquals(Code.UNKNOWN, qie.getErrorCode());

    qie = QueryException.wrapIfNeeded(roundTrip(new QueryException(new ISE("Something bad!"))));
    Assert.assertEquals("Something bad!", qie.getMessage());
    Assert.assertEquals("io.druid.java.util.common.ISE", qie.getErrorClass());
    Assert.assertEquals(Code.UNKNOWN, qie.getErrorCode());
    Assert.assertTrue(qie.getErrorStack().get(0).contains("QueryExceptionTest.testSerde"));
    Assert.assertFalse(qie.getErrorStack().stream().anyMatch(s -> s.contains("Caused by:")));

    qie = QueryException.wrapIfNeeded(
        roundTrip(new QueryException(new ISE(new RuntimeException("RE"), "Something bad!")))
    );
    Assert.assertEquals("Something bad!", qie.getMessage());
    Assert.assertEquals("io.druid.java.util.common.ISE", qie.getErrorClass());
    Assert.assertEquals(Code.UNKNOWN, qie.getErrorCode());
    Assert.assertTrue(qie.getErrorStack().get(0).contains("QueryExceptionTest.testSerde"));
    Assert.assertTrue(qie.getErrorStack().stream().anyMatch(s -> s.contains("Caused by:")));
  }

  private static QueryException roundTrip(final QueryException e)
  {
    try {
      return MAPPER.readValue(MAPPER.writeValueAsBytes(e), QueryException.class);
    }
    catch (Exception e2) {
      throw Throwables.propagate(e2);
    }
  }
}
