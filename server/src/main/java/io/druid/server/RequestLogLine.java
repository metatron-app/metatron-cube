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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;

public class RequestLogLine
{
  private final DateTime timestamp;
  private final String remoteAddr;
  private final Object query;
  private final QueryStats queryStats;

  public RequestLogLine(DateTime timestamp, String remoteAddr, Object query, QueryStats queryStats)
  {
    this.timestamp = timestamp;
    this.remoteAddr = remoteAddr;
    this.query = query;
    this.queryStats = queryStats;
  }

  public String getLine(ObjectMapper objectMapper) throws JsonProcessingException
  {
    return objectMapper.writeValueAsString(queryStats) + '\t' + objectMapper.writeValueAsString(query);
  }

  @JsonProperty("timestamp")
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty("query")
  public Object getQuery()
  {
    return query;
  }

  @JsonProperty("remoteAddr")
  public String getRemoteAddr()
  {
    return remoteAddr;
  }

  @JsonProperty("queryStats")
  public QueryStats getQueryStats()
  {
    return queryStats;
  }
}
