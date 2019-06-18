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

package io.druid.server.security.OAuth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class OAuthConfig
{
  public static final String OAUTH_CONFIG_PATH = "druid.oauth";
  public static final String PATH_KEY = "filterPath";
  public static final String VALIDATE_URL_KEY = "validateURLFormat";
  private static final String BROKER_QUERY_REQUEST_PATH_DEFAULT = "/druid/v2/*";


  @JsonProperty(PATH_KEY)
  private final String path;

  @JsonProperty(VALIDATE_URL_KEY)
  private final String validateURLFormat;

  @JsonCreator
  public OAuthConfig(
      @JsonProperty(PATH_KEY) String path,
      @JsonProperty(VALIDATE_URL_KEY) String validateURLFormat
  )
  {
    this.path = path == null ? BROKER_QUERY_REQUEST_PATH_DEFAULT : path;
    this.validateURLFormat = Preconditions.checkNotNull(validateURLFormat,
        "%s.%s should be specified", OAUTH_CONFIG_PATH, VALIDATE_URL_KEY);
  }

  public String getPath()
  {
    return path;
  }

  public String getValidateURLFormat()
  {
    return validateURLFormat;
  }
}
