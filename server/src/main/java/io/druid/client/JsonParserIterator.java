/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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
import com.metamx.common.IAE;
import com.metamx.common.RE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.QueryInterruptedException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

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
    catch (IOException e) {
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
        } else if (nextToken != JsonToken.START_ARRAY) {
          throw new IAE("Next token wasn't a START_ARRAY, was[%s]", jp.getCurrentToken());
        } else {
          jp.nextToken();
          objectCodec = jp.getCodec();
        }
      }
      catch (Exception e) {
        throw handleException(e);
      }
    }
  }

  protected abstract JsonParser createParser(JsonFactory factory) throws Exception;

  protected abstract RuntimeException handleException(Exception ex);

  public boolean close()
  {
    boolean normalClose = jp == null || jp.isClosed();
    CloseQuietly.close(jp);
    return normalClose;
  }

  public static class FromFutureStream<T> extends JsonParserIterator<T>
  {
    private final URL url;
    private final String type;
    private final Future<InputStream> future;

    public FromFutureStream(
        ObjectMapper mapper,
        JavaType typeRef,
        URL url,
        String type,
        Future<InputStream> future
    )
    {
      super(mapper, typeRef);
      this.url = url;
      this.type = type;
      this.future = future;
    }

    @Override
    protected RuntimeException handleException(Exception ex)
    {
      if (ex instanceof IOException ||
          ex instanceof InterruptedException ||
          ex instanceof QueryInterruptedException ||
          ex instanceof CancellationException) {
        throw QueryInterruptedException.wrapIfNeeded(ex, url.getHost() + ":" + url.getPort(), type);
      }
      throw new RE(ex, "Failure getting results from[%s] because of [%s]", url, ex.getMessage());
    }

    protected JsonParser createParser(JsonFactory factory) throws Exception
    {
      return factory.createParser(future.get());
    }
  }
}
