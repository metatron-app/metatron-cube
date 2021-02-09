/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.http.client;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.handler.codec.base64.Base64;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class Request
{
  private static final ChannelBufferFactory factory = HeapChannelBufferFactory.getInstance();

  private final HttpMethod method;
  private final URL url;
  private final boolean eagerResponse;

  private final Multimap<String, String> headers = Multimaps.newListMultimap(
      Maps.<String, Collection<String>>newHashMap(),
      new Supplier<List<String>>() {
        @Override
        public List<String> get() {
          return Lists.newArrayList();
        }
      }
  );

  private ChannelBuffer content;

  public Request(HttpMethod method, URL url)
  {
    this(method, url, false);
  }

  public Request(HttpMethod method, URL url, boolean eagerResponse)
  {
    this.method = method;
    this.url = url;
    this.eagerResponse = eagerResponse;
  }

  public HttpMethod getMethod()
  {
    return method;
  }

  public URL getUrl()
  {
    return url;
  }

  public boolean isEagerResponse()
  {
    return eagerResponse;
  }

  public Multimap<String, String> getHeaders()
  {
    return headers;
  }

  public boolean hasContent()
  {
    return content != null;
  }

  public ChannelBuffer getContent()
  {
    return content;
  }

  public Request copy() {
    Request retVal = new Request(method, url, eagerResponse);
    retVal.headers.putAll(headers);
    retVal.content = content == null ? null : content.copy();
    return retVal;
  }

  public Request setHeader(String header, String value)
  {
    headers.replaceValues(header, Arrays.asList(value));
    return this;
  }

  public Request setHeaderValues(String header, Iterable<String> value)
  {
    headers.replaceValues(header, value);
    return this;
  }

  public Request setHeaderValues(Multimap<String, String> inHeaders) {
    for (Map.Entry<String, Collection<String>> entry : inHeaders.asMap().entrySet()) {
      this.setHeaderValues(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public Request addHeader(String header, String value)
  {
    headers.put(header, value);
    return this;
  }

  public Request addHeaderValues(String header, Iterable<String> value)
  {
    headers.putAll(header, value);
    return this;
  }

  public Request addHeaderValues(Multimap<String, String> inHeaders) {
    for (Map.Entry<String, Collection<String>> entry : inHeaders.asMap().entrySet()) {
      this.addHeaderValues(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public Request setContent(byte[] bytes)
  {
    return setContent(null, bytes);
  }

  public Request setContent(byte[] bytes, int offset, int length)
  {
    return setContent(null, bytes, offset, length);
  }

  public Request setContent(ChannelBuffer content)
  {
    return setContent(null, content);
  }

  public Request setContent(String contentType, byte[] bytes)
  {
    return setContent(contentType, bytes, 0, bytes.length);
  }

  public Request setContent(String contentType, byte[] bytes, int offset, int length)
  {
    return setContent(contentType, factory.getBuffer(bytes, offset, length));
  }

  public Request setContent(String contentType, ChannelBuffer content)
  {
    if (contentType != null) {
      setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType);
    }

    this.content = content;

    setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(content.writerIndex()));

    return this;
  }

  public Request setBasicAuthentication(String username, String password)
  {
    final String base64Value = base64Encode(String.format("%s:%s", username, password));
    setHeader(HttpHeaders.Names.AUTHORIZATION, String.format("Basic %s", base64Value));
    return this;
  }

  private String base64Encode(final String value)
  {
    final ChannelBufferFactory bufferFactory = HeapChannelBufferFactory.getInstance();

    return Base64
        .encode(bufferFactory.getBuffer(ByteBuffer.wrap(value.getBytes(Charsets.UTF_8))), false)
        .toString(Charsets.UTF_8);
  }
}