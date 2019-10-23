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
package io.druid.server.security.OAuth;

import com.amazonaws.util.json.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentMap;

public class OAuthTokenChecker
{
  private static final Logger log = new Logger(OAuthTokenChecker.class);

  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final String URLFormat;
  private final ConcurrentMap<String, OAuthTokenInfo> cachedTokens;
  private final HttpClient httpClient;

  public OAuthTokenChecker(
      String URLFormat,
      HttpClient httpClient
  )
  {
    this.URLFormat = URLFormat;
    cachedTokens = Maps.newConcurrentMap();
    this.httpClient = httpClient;
  }

  public boolean isValid(String accessToken)
  {
    OAuthTokenInfo token = cachedTokens.get(accessToken);
    if (token != null) {
      if (token.getExpireTime() > System.currentTimeMillis()) {
        // TODO - more validation
        return true;
      }
      // it's stale token
      cachedTokens.remove(accessToken);
    }
    // it needs to check through validation server
    URL validationURL = validationURL(accessToken);
    try{
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              validationURL
          ),
          RESPONSE_HANDLER
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        return false;
      }

      JSONObject jsonObject = new JSONObject(response.getContent());
      if(jsonObject.tryGetString("error") != null) {
        return false;
      }
      // sec to msec conversion
      Long expireTime = jsonObject.getLong("exp") * 1000;
      String clientID = jsonObject.tryGetString("client_id");
      String userName = jsonObject.tryGetString("user_name");

      OAuthTokenInfo tokenInfo = new OAuthTokenInfo(
          accessToken,
          expireTime,
          clientID,
          userName
      );
      cachedTokens.putIfAbsent(accessToken, tokenInfo);

      return true;
    }
    catch(Exception e)
    {
      log.error("Cannot get validation info from %s - %s", validationURL, e.getMessage());

      return false;
    }
  }

  private URL validationURL(String accessToken)
  {
    String urlString = String.format(URLFormat, accessToken);
    try {
      return new URL(urlString);
    }
    catch (MalformedURLException e) {
      throw new IAE("malformed URL - %s", urlString);
    }
  }
}

