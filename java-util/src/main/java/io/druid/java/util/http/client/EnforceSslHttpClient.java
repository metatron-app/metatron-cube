/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.java.util.http.client;

import io.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

import java.net.URL;

/**
 */
public class EnforceSslHttpClient implements HttpClient
{
  private final HttpClient delegate;

  public EnforceSslHttpClient(HttpClient delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public <Intermediate, Final> ChannelResource<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration requestReadTimeout
  )
  {
    URL url = request.getUrl();

    if (!"https".equals(url.getProtocol())) {
      throw new IllegalArgumentException(String.format("Requests must be over https, got[%s].", url));
    }

    return delegate.go(request, handler, requestReadTimeout);
  }
}
