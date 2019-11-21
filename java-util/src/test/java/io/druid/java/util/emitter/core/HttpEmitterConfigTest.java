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

public class HttpEmitterConfigTest
{
  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    props.put("io.druid.java.util.emitter.recipientBaseUrl", "http://example.com/");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        HttpEmitterConfig.class
    );

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(500, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals(null, config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
    Assert.assertEquals(2.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(0, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testDefaultsLegacy()
  {
    final Properties props = new Properties();
    props.put("io.druid.java.util.emitter.http.url", "http://example.com/");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(Emitters.makeHttpMap(props), HttpEmitterConfig.class);

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(300, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals(null, config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
    Assert.assertEquals(2.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(0, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("io.druid.java.util.emitter.flushMillis", "1");
    props.setProperty("io.druid.java.util.emitter.flushCount", "2");
    props.setProperty("io.druid.java.util.emitter.recipientBaseUrl", "http://example.com/");
    props.setProperty("io.druid.java.util.emitter.basicAuthentication", "a:b");
    props.setProperty("io.druid.java.util.emitter.batchingStrategy", "NEWLINES");
    props.setProperty("io.druid.java.util.emitter.maxBatchSize", "4");
    props.setProperty("io.druid.java.util.emitter.flushTimeOut", "1000");
    props.setProperty("io.druid.java.util.emitter.batchQueueSizeLimit", "2500");
    props.setProperty("io.druid.java.util.emitter.httpTimeoutAllowanceFactor", "3.0");
    props.setProperty("io.druid.java.util.emitter.minHttpTimeoutMillis", "100");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        HttpEmitterConfig.class
    );

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
    Assert.assertEquals(2500, config.getBatchQueueSizeLimit());
    Assert.assertEquals(3.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(100, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testSettingEverythingLegacy()
  {
    final Properties props = new Properties();
    props.setProperty("io.druid.java.util.emitter.flushMillis", "1");
    props.setProperty("io.druid.java.util.emitter.flushCount", "2");
    props.setProperty("io.druid.java.util.emitter.http.url", "http://example.com/");
    props.setProperty("io.druid.java.util.emitter.http.basicAuthentication", "a:b");
    props.setProperty("io.druid.java.util.emitter.http.batchingStrategy", "newlines");
    props.setProperty("io.druid.java.util.emitter.http.maxBatchSize", "4");
    props.setProperty("io.druid.java.util.emitter.http.flushTimeOut", "1000");
    props.setProperty("io.druid.java.util.emitter.http.batchQueueSizeLimit", "2500");
    props.setProperty("io.druid.java.util.emitter.http.httpTimeoutAllowanceFactor", "3.0");
    props.setProperty("io.druid.java.util.emitter.http.minHttpTimeoutMillis", "100");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(Emitters.makeHttpMap(props), HttpEmitterConfig.class);

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
    Assert.assertEquals(2500, config.getBatchQueueSizeLimit());
    Assert.assertEquals(3.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(100, config.getMinHttpTimeoutMillis());
  }

}