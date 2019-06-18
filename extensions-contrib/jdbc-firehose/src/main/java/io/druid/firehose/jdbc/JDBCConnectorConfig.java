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
package io.druid.firehose.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JDBCConnectorConfig
{
  @JsonProperty
  private String host = "localhost";

  @JsonProperty
  private int port = 1527;

  @JsonProperty
  private String connectURI;

  @JsonProperty
  private String user;

  @JsonProperty
  private String password;

  @JsonProperty
  private String driverClass;

  public JDBCConnectorConfig()
  {
  }

  public JDBCConnectorConfig(String host, int port, String connectURI, String user, String password, String driverClass)
  {
    this.host = host;
    this.port = port;
    this.connectURI = connectURI;
    this.user = user;
    this.password = password;
    this.driverClass = driverClass;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public String getConnectURI()
  {
    return connectURI;
  }

  public String getUser()
  {
    return user;
  }

  public String getPassword()
  {
    return password;
  }

  public String getDriverClass()
  {
    return driverClass;
  }

  @Override
  public String toString()
  {
    return "JDBCConnectorConfig{" +
           "connectURI='" + getConnectURI() + '\'' +
           ", user='" + getUser() + '\'' +
           ", password='" + getPassword() + '\'' +
           ", driverClass='" + getDriverClass() + '\'' +
           "}";
  }
}
