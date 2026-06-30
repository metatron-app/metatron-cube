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

package io.druid.segwriter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.aggregation.AggregatorFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Declarative ingestion spec for the spec-driven Spark writer. Parse it with Druid's
 * DefaultObjectMapper so {@code metrics} accept native aggregator JSON
 * (e.g. {@code {"type":"longSum","name":"added","fieldName":"added"}}).
 *
 * Credentials are NOT in the spec: the S3 pusher uses the default chain
 * (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env), injected from a k8s secret.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentIngestSpec implements Serializable
{
  private final String dataSource;
  // where the rows come from: {"type":"file",...} or {"type":"iceberg",...}. Consumed by the
  // spark-ingestion module (which performs the actual Spark read); unused by segment building.
  private final SourceSpec source;
  private final String timestampColumn;
  private final List<String> dimensions;
  private final AggregatorFactory[] metrics;
  private final String segmentGranularity;
  private final String queryGranularity;
  private final boolean rollup;
  private final int numShards;
  private final String bucket;
  private final String baseKey;
  private final String endpoint;
  private final String region;
  private final boolean disableAcl;
  private final String publishUrl;
  // column -> raw secondary-index spec JSON (e.g. {"type":"lucene10","strategies":[{"type":"text","fieldName":"x"}]}).
  // Kept raw here so the spec parses without the polymorphic SecondaryIndexingSpec subtypes; converted
  // to SecondaryIndexingSpec in SegmentIngestor with a mapper that has the lucene subtypes registered.
  private final Map<String, Map<String, Object>> secondaryIndexing;

  @JsonCreator
  public SegmentIngestSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("source") SourceSpec source,
      @JsonProperty("timestampColumn") String timestampColumn,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") AggregatorFactory[] metrics,
      @JsonProperty("segmentGranularity") String segmentGranularity,
      @JsonProperty("queryGranularity") String queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("numShards") Integer numShards,
      @JsonProperty("bucket") String bucket,
      @JsonProperty("baseKey") String baseKey,
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("region") String region,
      @JsonProperty("disableAcl") Boolean disableAcl,
      @JsonProperty("publishUrl") String publishUrl,
      @JsonProperty("secondaryIndexing") Map<String, Map<String, Object>> secondaryIndexing
  )
  {
    this.dataSource = dataSource;
    this.source = source;
    this.timestampColumn = timestampColumn;
    this.dimensions = dimensions;
    this.metrics = metrics == null ? new AggregatorFactory[0] : metrics;
    this.segmentGranularity = segmentGranularity == null ? "DAY" : segmentGranularity;
    this.queryGranularity = queryGranularity == null ? "NONE" : queryGranularity;
    this.rollup = rollup == null ? true : rollup;
    this.numShards = numShards == null ? 1 : numShards;
    this.bucket = bucket;
    this.baseKey = baseKey == null ? "druid/segments" : baseKey;
    this.endpoint = endpoint;
    this.region = region;
    this.disableAcl = disableAcl == null ? true : disableAcl;
    this.publishUrl = publishUrl;
    this.secondaryIndexing = secondaryIndexing == null
                             ? java.util.Collections.<String, Map<String, Object>>emptyMap()
                             : secondaryIndexing;
  }

  @JsonProperty public String getDataSource() { return dataSource; }
  @JsonProperty public SourceSpec getSource() { return source; }
  @JsonProperty public String getTimestampColumn() { return timestampColumn; }
  @JsonProperty public List<String> getDimensions() { return dimensions; }
  @JsonProperty public AggregatorFactory[] getMetrics() { return metrics; }
  @JsonProperty public String getSegmentGranularity() { return segmentGranularity; }
  @JsonProperty public String getQueryGranularity() { return queryGranularity; }
  @JsonProperty public boolean isRollup() { return rollup; }
  @JsonProperty public int getNumShards() { return numShards; }
  @JsonProperty public String getBucket() { return bucket; }
  @JsonProperty public String getBaseKey() { return baseKey; }
  @JsonProperty public String getEndpoint() { return endpoint; }
  @JsonProperty public String getRegion() { return region; }
  @JsonProperty public boolean isDisableAcl() { return disableAcl; }
  @JsonProperty public String getPublishUrl() { return publishUrl; }
  @JsonProperty public Map<String, Map<String, Object>> getSecondaryIndexing() { return secondaryIndexing; }
}
