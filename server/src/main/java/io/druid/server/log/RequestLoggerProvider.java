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

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import com.metamx.common.logger.Logger;
import io.druid.server.RequestLogLine;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * A Marker interface for things that can provide a RequestLogger.  This can be combined with jackson polymorphic serde
 * to provide new RequestLogger implementations as plugins.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopRequestLoggerProvider.class)
public interface RequestLoggerProvider extends Provider<RequestLogger>
{
  @JsonTypeName("log")
  public static class Logging implements RequestLoggerProvider
  {
    private static final Logger log = new Logger(RequestLogger.class);

    @JacksonInject
    @NotNull
    private ObjectMapper jsonMapper = null;

    @Override
    public RequestLogger get()
    {
      final ObjectMapper mapper = jsonMapper.copy().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

      return new RequestLogger()
      {
        @Override
        public void log(RequestLogLine requestLogLine) throws IOException
        {
          log.info(requestLogLine.getLine(mapper));
        }
      };
    }
  }
}
