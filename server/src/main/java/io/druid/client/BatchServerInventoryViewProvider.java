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

package io.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 */
public class BatchServerInventoryViewProvider implements ServerInventoryViewProvider
{
  @JacksonInject
  @NotNull
  private ZkPathsConfig zkPaths = null;

  @JacksonInject
  @NotNull
  private CuratorFramework curator = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @JacksonInject
  @NotNull
  private MetadataSegmentManager segmentManager;

  @Override
  public BatchServerInventoryView get()
  {
    ObjectMapper mapper = jsonMapper;
    if (segmentManager != null) {
      mapper = DefaultObjectMapper.withDeserializer(mapper, DataSegment.class, toDedupDeserializer(mapper));
    }
    return newInventoryView(mapper);
  }

  private JsonDeserializer<DataSegment> toDedupDeserializer(final ObjectMapper mapper)
  {
    return new JsonDeserializer<DataSegment>()
    {
      @Override
      public DataSegment deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
      {
        return segmentManager.dedupSegment(mapper.readValue(jp, DataSegment.class));
      }
    };
  }

  private BatchServerInventoryView newInventoryView(ObjectMapper mapper)
  {
    return new BatchServerInventoryView(
        zkPaths,
        curator,
        mapper,
        Predicates.<Pair<DruidServerMetadata, DataSegment>>alwaysTrue()
    );
  }
}
