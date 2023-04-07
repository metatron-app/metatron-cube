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

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.druid.query.QueryException;

import java.util.Map;

public class ObjectMappers
{
  public static final TypeReference<Map<String, Object>> MAP_REF = new TypeReference<Map<String, Object>>() {};

  public static ObjectMapper excludeNulls(ObjectMapper mapper)
  {
    return mapper.copy().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
  }

  public static <T> ObjectMapper withDeserializer(ObjectMapper mapper, Class<T> clazz, JsonDeserializer<T> deserializer)
  {
    return mapper.copy().registerModule(new SimpleModule().addDeserializer(clazz, deserializer));
  }

  public static byte[] writeBytes(ObjectMapper objectMapper, Object value)
  {
    try {
      return objectMapper.writeValueAsBytes(value);
    }
    catch (JsonProcessingException e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }
}
