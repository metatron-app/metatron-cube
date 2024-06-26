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

package io.druid.server.security;

import io.druid.common.utils.StringUtils;

public class Access
{
  public static final Access OK = new Access(true);

  private final boolean allowed;
  private final String message;

  public Access(boolean allowed)
  {
    this(allowed, "");
  }

  public Access(boolean allowed, String message)
  {
    this.allowed = allowed;
    this.message = message;
  }

  public boolean isAllowed()
  {
    return allowed;
  }

  public String getMessage()
  {
    return message;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("Allowed:%s, Message:%s", allowed, message);
  }
}
