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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.RE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.QueryInterruptedException;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 */
public class JsonParserIterator<T> implements Iterator<T>, Closeable
{
  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final ObjectMapper mapper;
  private final JavaType typeRef;
  private final Future<InputStream> future;
  private final URL url;

  public JsonParserIterator(ObjectMapper mapper, JavaType typeRef, Future<InputStream> future, URL url)
  {
    this.mapper = mapper;
    this.typeRef = typeRef;
    this.future = future;
    this.url = url;
    jp = null;
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
      throw Throwables.propagate(e);
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
        jp = mapper.getFactory().createParser(future.get());
        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_OBJECT) {
          QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
          throw new QueryInterruptedException(cause, url.getHost() + ":" + url.getPort());
        } else if (nextToken != JsonToken.START_ARRAY) {
          throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
        } else {
          jp.nextToken();
          objectCodec = jp.getCodec();
        }
      }
      catch (IOException | InterruptedException | ExecutionException e) {
        throw new RE(e, "Failure getting results from[%s] because of [%s]", url, e.getMessage());
      }
      catch (CancellationException e) {
        throw new QueryInterruptedException(e, url.getHost() + ":" + url.getPort());
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
  }
}
