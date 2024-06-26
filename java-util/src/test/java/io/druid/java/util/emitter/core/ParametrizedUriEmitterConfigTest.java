/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
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

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ParametrizedUriEmitterConfigTest
{
  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();

    final ObjectMapper objectMapper = new ObjectMapper();
    final ParametrizedUriEmitterConfig paramConfig = objectMapper.convertValue(Emitters.makeCustomFactoryMap(props), ParametrizedUriEmitterConfig.class);
    final HttpEmitterConfig config = paramConfig.buildHttpEmitterConfig("http://example.com/topic");

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(500, config.getFlushCount());
    Assert.assertEquals("http://example.com/topic", config.getRecipientBaseUrl());
    Assert.assertEquals(null, config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("io.druid.java.util.emitter.httpEmitting.flushMillis", "1");
    props.setProperty("io.druid.java.util.emitter.httpEmitting.flushCount", "2");
    props.setProperty("io.druid.java.util.emitter.httpEmitting.basicAuthentication", "a:b");
    props.setProperty("io.druid.java.util.emitter.httpEmitting.batchingStrategy", "NEWLINES");
    props.setProperty("io.druid.java.util.emitter.httpEmitting.maxBatchSize", "4");
    props.setProperty("io.druid.java.util.emitter.httpEmitting.flushTimeOut", "1000");

    final ObjectMapper objectMapper = new ObjectMapper();
    final ParametrizedUriEmitterConfig paramConfig = objectMapper.convertValue(Emitters.makeCustomFactoryMap(props), ParametrizedUriEmitterConfig.class);
    final HttpEmitterConfig config = paramConfig.buildHttpEmitterConfig("http://example.com/topic");

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/topic", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
  }
}
