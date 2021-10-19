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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.granularity.Granularity;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.FileLoadSpec;
import io.druid.server.TypeStringResolver;

import java.io.IOException;

@JsonTypeName("excel")
public class ExcelLoadSpecResolver extends TypeStringResolver
{
  @JsonCreator
  public ExcelLoadSpecResolver(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") String paths,
      @JsonProperty("recursive") boolean recursive,
      @JsonProperty("typeString") String typeString,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    super(basePath, paths, recursive, typeString, timeExpression, segmentGranularity);
  }

  @Override
  public FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    FileLoadSpec resolved = super.resolve(dataSource, walker);
    return resolved.withExternalFormat("druid-indexing-hadoop", "io.druid.indexer.hadoop.ExcelRowInputFormat");
  }
}
