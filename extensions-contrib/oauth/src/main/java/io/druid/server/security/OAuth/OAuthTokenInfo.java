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

import com.google.common.base.Preconditions;

public class OAuthTokenInfo
{
  private String accessToken;
  private long expireTime;
  private String clientID;
  private String userName;

  public OAuthTokenInfo(
    String accessToken,
    long expireTime,
    String clientID,
    String userName
  )
  {
    final long current = System.currentTimeMillis();
    this.accessToken = Preconditions.checkNotNull(accessToken, "accessToken should be supplied");
    Preconditions.checkArgument(expireTime > current,
        "expireTime should be set as future but expireTime: %d, current: %d", expireTime, current);
    this.expireTime = expireTime;
    this.clientID = clientID;
    this.userName = userName;
  }

  public long getExpireTime()
  {
    return expireTime;
  }

  public String getAccessToken()
  {
    return accessToken;
  }

  public String getClientID()
  {
    return clientID;
  }

  public String getUserName()
  {
    return userName;
  }
}
