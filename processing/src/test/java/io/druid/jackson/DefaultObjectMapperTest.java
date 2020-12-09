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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DefaultObjectMapperTest
{
  ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testDateTime() throws Exception
  {
    final DateTime time = new DateTime();

    Assert.assertEquals(String.format("\"%s\"", time), mapper.writeValueAsString(time));
  }

  @Test
  public void testSequence() throws Exception
  {
    List<String> values = Arrays.asList("a", "b", "c", "d");
    Sequence<String> sequence = Sequences.simple(values);
    Sequence<String> read = mapper.readValue(
        mapper.writeValueAsBytes(sequence), new TypeReference<Sequence<String>>()
        {
        }
    );
    Assert.assertEquals(values, Sequences.toList(read));

    read = mapper.readValue(
        mapper.writeValueAsString(sequence), new TypeReference<Sequence<String>>()
        {
        }
    );
    Assert.assertEquals(values, Sequences.toList(read));
  }
}
