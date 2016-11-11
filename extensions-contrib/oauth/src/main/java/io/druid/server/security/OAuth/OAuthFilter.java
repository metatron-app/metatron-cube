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
package io.druid.server.security.OAuth;

import com.metamx.http.client.HttpClient;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;

public class OAuthFilter implements Filter
{
  private volatile FilterConfig config;
  private final OAuthTokenChecker tokenChecker;

  public OAuthFilter(
      HttpClient httpClient,
      String checkURLFormat
  )
  {
    this.tokenChecker = new OAuthTokenChecker(
        checkURLFormat,
        httpClient
    );
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {
    this.config = config;
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
      throws IOException, ServletException
  {
    HttpServletRequest httpServletRequest = (HttpServletRequest)req;
    String accessToken = parseAccessToken(httpServletRequest);
    if (accessToken != null && tokenChecker.isValid(accessToken)) {
      chain.doFilter(req, resp);
    } else {
      HttpServletResponse response = (HttpServletResponse) resp;
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Failed authentication.");
    }
  }

  @Override
  public void destroy()
  {
  }

  private String parseAccessToken(HttpServletRequest req)
  {
    String accessToken = parseParameter(req);
    if (accessToken == null) {
      accessToken = parseHeader(req);
    }

    return accessToken;
  }

  static String TOKEN_PARAMETER = "access_token";
  static String TOKEN_HEADER = "Authorization";
  static String TOKEN_HEADER_PREFIX = "Bearer ";

  private String parseParameter(HttpServletRequest req)
  {
    String accessToken = null;

    Enumeration<String> params = req.getParameterNames();
    while(params.hasMoreElements()) {
      String param = params.nextElement();
      if (param.equalsIgnoreCase(TOKEN_PARAMETER)) {
        accessToken = req.getParameter(param);
      }
    }

    return accessToken;
  }

  private String parseHeader(HttpServletRequest req)
  {
    String accessToken = null;

    String authHeader = req.getHeader(TOKEN_HEADER);
    if (authHeader != null && authHeader.startsWith(TOKEN_HEADER_PREFIX)) {
      accessToken = authHeader.substring(TOKEN_HEADER_PREFIX.length());
    }

    return accessToken;
  }
}
