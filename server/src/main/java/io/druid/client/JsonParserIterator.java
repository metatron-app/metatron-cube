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

package io.druid.client;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.QueryInterruptedException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 */
public abstract class JsonParserIterator<T> implements Iterator<T>
{
  private final ObjectMapper mapper;
  private final JavaType typeRef;

  private JsonParser jp;
  private ObjectCodec objectCodec;

  public JsonParserIterator(ObjectMapper mapper, JavaType typeRef)
  {
    this.mapper = mapper;
    this.typeRef = typeRef;
  }

  @Override
  public boolean hasNext()
  {
    init();

    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseQuietly.close(jp);
      return false;
    }

    return true;
  }

  @Override
  public T next()
  {
    init();
    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
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
        jp = createParser(mapper.getFactory());
        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_OBJECT) {
          throw jp.getCodec().readValue(jp, QueryInterruptedException.class);
        } else if (nextToken == JsonToken.VALUE_STRING) {
          throw new IAE("Next token wasn't a START_ARRAY, was string [%s]", StringUtils.limit(jp.getText(), 256));
        } else if (nextToken != JsonToken.START_ARRAY) {
          throw new IAE("Next token wasn't a START_ARRAY, was[%s]", jp.getCurrentToken());
        } else {
          jp.nextToken();
          objectCodec = jp.getCodec();
        }
      }
      catch (Throwable e) {
        throw handleException(e);
      }
    }
  }

  protected abstract JsonParser createParser(JsonFactory factory) throws Exception;

  protected abstract RuntimeException handleException(Throwable ex);

  public boolean close()
  {
    // jp null : timeout, maybe
    boolean normalClose = jp != null && jp.isClosed();
    CloseQuietly.close(jp);
    return normalClose;
  }

  public static class FromCallable<T> extends JsonParserIterator<T>
  {
    private final URL url;
    private final String type;
    private final Callable<InputStream> callable;

    public FromCallable(
        ObjectMapper mapper,
        JavaType typeRef,
        URL url,
        String type,
        Callable<InputStream> callable
    )
    {
      super(mapper, typeRef);
      this.url = url;
      this.type = type;
      this.callable = callable;
    }

    @Override
    protected final RuntimeException handleException(final Throwable t)
    {
      for (Throwable ex = t; ex != null; ex = ex.getCause()) {
        if (ex instanceof TimeoutException ||
            ex instanceof InterruptedException ||
            ex instanceof QueryInterruptedException ||
            ex instanceof org.jboss.netty.handler.timeout.TimeoutException ||
            ex instanceof CancellationException) {
          // todo should retry to other replica if exists?
          throw QueryInterruptedException.wrapIfNeeded(ex, url.getHost() + ":" + url.getPort(), type);
        }
      }
      throw new RE(t, "Failure getting results from[%s] because of [%s]", url, t.getMessage());
    }

    @Override
    protected final JsonParser createParser(JsonFactory factory) throws Exception
    {
      return factory.createParser(callable.call());
    }
  }
}
