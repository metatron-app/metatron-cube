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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = HadoopPathSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "granular_unprocessed", value = GranularUnprocessedPathSpec.class),
    @JsonSubTypes.Type(name = "granularity", value = GranularityPathSpec.class),
    @JsonSubTypes.Type(name = "static", value = StaticPathSpec.class),
    @JsonSubTypes.Type(name = "hadoop", value = HadoopPathSpec.class),
    @JsonSubTypes.Type(name = "dataSource", value = DatasourcePathSpec.class),
    @JsonSubTypes.Type(name = "multi", value = MultiplePathSpec.class),
    @JsonSubTypes.Type(name = "partition", value = PartitionPathSpec.class)
})
public interface PathSpec
{
  Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException;

  interface Resolving extends PathSpec
  {
    PathSpec resolve();
  }
}
