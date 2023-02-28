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

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryException;
import io.netty.channel.ChannelException;

import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 */
public class JsonParserIterator<T> implements Iterator<T>
{
  private static final Logger LOG = new Logger(JsonParserIterator.class);

  private final String host;
  private final String type;
  private final Callable<InputStream> callable;
  private final ObjectMapper mapper;
  private final JavaType typeRef;

  private JsonParser jp;
  private ObjectCodec objectCodec;

  public JsonParserIterator(ObjectMapper mapper, JavaType typeRef, String host, String type, Callable<InputStream> callable)
  {
    this.host = host;
    this.type = type;
    this.callable = callable;
    this.mapper = mapper;
    this.typeRef = typeRef;
  }

  public JsonParserIterator(ObjectMapper mapper, JavaType typeRef, URL url, String type, Callable<InputStream> callable)
  {
    this(mapper, typeRef, url.getHost() + ":" + url.getPort(), type, callable);
  }

  @Override
  public boolean hasNext()
  {
    init();
    return !jp.isClosed();
  }

  @Override
  public T next()
  {
    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      if (jp.nextToken() == JsonToken.END_ARRAY) {
        CloseQuietly.close(jp);
      }
      return retVal;
    }
    catch (Throwable e) {
      throw handleException(e);
    }
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  private void init()
  {
    if (jp == null) {
      try {
        jp = mapper.getFactory().createParser(callable.call());
        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_OBJECT) {
          Map<String, Object> map = jp.readValueAs(new TypeReference<Map<String, Object>>(){});
          QueryException qe = mapper.convertValue(map, QueryException.class);
          if (!map.isEmpty() && qe.getMessage() == null) {
            LOG.warn("Null error message in QueryException from value %s", map);
          }
          throw qe;
        } else if (nextToken == JsonToken.VALUE_STRING) {
          throw new IAE("Next token wasn't a START_ARRAY, was string [%s]", StringUtils.limit(jp.getText(), 256));
        } else if (nextToken != JsonToken.START_ARRAY) {
          throw new IAE("Next token wasn't a START_ARRAY, was[%s]", jp.getCurrentToken());
        } else {
          if (jp.nextToken() == JsonToken.END_ARRAY) {
            CloseQuietly.close(jp);
          } else {
            objectCodec = jp.getCodec();
          }
        }
      }
      catch (Throwable e) {
        throw handleException(e);
      }
    }
  }

  private RuntimeException handleException(final Throwable t)
  {
    for (Throwable ex = t; ex != null; ex = ex.getCause()) {
      if (ex instanceof TimeoutException ||
          ex instanceof InterruptedException ||
          ex instanceof QueryException ||
          ex instanceof org.jboss.netty.handler.timeout.TimeoutException ||
          ex instanceof CancellationException ||
          ex instanceof ChannelException) {
        // todo should retry to other replica if exists?
        throw QueryException.wrapIfNeeded(ex, host, type);
      }
    }
    throw new RE(t, "Failure getting results from[%s(%s)] because of [%s]", host, type, t.getMessage());
  }

  public boolean close()
  {
    // jp null : timeout, maybe
    boolean normalClose = jp != null && jp.isClosed();
    CloseQuietly.close(jp);
    return normalClose;
  }
}
