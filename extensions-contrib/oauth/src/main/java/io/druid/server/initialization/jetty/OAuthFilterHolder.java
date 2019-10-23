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
package io.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.http.client.HttpClient;
import io.druid.guice.annotations.Global;
import io.druid.server.security.OAuth.OAuthConfig;
import io.druid.server.security.OAuth.OAuthFilter;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

public class OAuthFilterHolder implements ServletFilterHolder
{
  private final HttpClient httpClient;
  private final OAuthConfig oAuthConfig;

  @Inject
  public OAuthFilterHolder(
      @Global HttpClient httpClient,
      OAuthConfig oAuthConfig
  )
  {
    this.httpClient = httpClient;
    this.oAuthConfig = oAuthConfig;
  }

  @Override
  public Filter getFilter()
  {
    return new OAuthFilter(httpClient, oAuthConfig.getValidateURLFormat());
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return ImmutableMap.of();
  }

  @Override
  public String getPath()
  {
    return oAuthConfig.getPath();
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }


}
