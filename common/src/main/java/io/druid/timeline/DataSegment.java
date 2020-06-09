/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.common.utils.JodaUtils;
import io.druid.jackson.CommaListJoinSerializer;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class DataSegment implements Comparable<DataSegment>
{
  public static final Map<String, Object> MASKED = ImmutableMap.of("masked", "intentionally");

  public static final JsonDeserializer<DataSegment> DS_SKIP_LOADSPEC = new JsonDeserializer<DataSegment>()
  {
    @Override
    public DataSegment deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
    {
      Preconditions.checkArgument(jp.getCurrentToken() == JsonToken.START_OBJECT, "not object?");
      JsonNode root = jp.readValueAsTree();

      String dataSource = root.get("dataSource").asText();
      Interval interval = new Interval(root.get("interval").asText());
      String version = root.get("version").asText();
      String dimensions = root.get("dimensions").asText();
      String metrics = root.get("metrics").asText();

      ShardSpec shardSpec = jp.getCodec().treeToValue(root.get("shardSpec"), ShardSpec.class);
      JsonNode binaryVersionNode = root.get("binaryVersion");
      Integer binaryVersion = binaryVersionNode == null || binaryVersionNode.isNull() ? null : binaryVersionNode.asInt();
      long size = root.get("size").asLong();
      int numRows = root.get("numRows").asInt();

      return new DataSegment(
          dataSource,
          interval,
          version,
          MASKED,
          dimensions,
          metrics,
          shardSpec,
          binaryVersion,
          size,
          numRows
      );
    }
  };

  public static final Comparator<DataSegment> TIME_DESCENDING = Ordering.from(JodaUtils.intervalsByEndThenStart())
                                                                        .onResultOf(DataSegment::getInterval)
                                                                        .compound(Ordering.<DataSegment>natural())
                                                                        .reverse();

  public static final String DELIMITER = "_";

  private static final DataSourceInterner DS_INTERNER = new DataSourceInterner();
  private static final Interner<String> ID_INTERNER = Interners.newWeakInterner();

  public static String toSegmentId(String dataSource, Interval interval, String version, int partitionNum)
  {
    final StringBuilder sb = new StringBuilder(196)
        .append(dataSource).append(DELIMITER)
        .append(interval.getStart()).append(DELIMITER)
        .append(interval.getEnd()).append(DELIMITER)
        .append(version);

    if (partitionNum != 0) {
      sb.append(DELIMITER).append(partitionNum);
    }

    return ID_INTERNER.intern(sb.toString());
  }

  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final Map<String, Object> loadSpec;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final ShardSpec shardSpec;
  private final Integer binaryVersion;
  private final long size;

  private final String identifier;

  // allow update cause it's only set in segments reproted from historical nodes
  // see MetadataSegmentManager.dedupSegment()
  private int numRows;

  private DataSegment()
  {
    this.dataSource = null;
    this.interval = null;
    this.version = null;
    this.loadSpec = null;
    this.dimensions = null;
    this.metrics = null;
    this.shardSpec = null;
    this.size = 0;
    this.binaryVersion = null;
    this.identifier = null;
  }

  @JsonCreator
  public DataSegment(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") Map<String, Object> loadSpec,
      @JsonProperty("dimensions") String dimensions,
      @JsonProperty("metrics") String metrics,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JsonProperty("numRows") int numRows
  )
  {
    this(
        DS_INTERNER.get(dataSource),
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        binaryVersion,
        size,
        numRows
    );
  }

  private DataSegment(
      DataSourceInterner.Intern intern,
      Interval interval,
      String version,
      Map<String, Object> loadSpec,
      String dimensions,
      String metrics,
      ShardSpec shardSpec,
      Integer binaryVersion,
      long size,
      int numRows
  )
  {
    this(
        intern.dataSource,
        interval,
        version,
        loadSpec,
        intern.intern(dimensions),
        intern.intern(metrics),
        shardSpec,
        binaryVersion,
        size,
        numRows
    );
  }

  public DataSegment(
      String dataSource,
      Interval interval,
      String version,
      Map<String, Object> loadSpec,
      List<String> dimensions,
      List<String> metrics,
      ShardSpec shardSpec,
      Integer binaryVersion,
      long size,
      int numRows
  )
  {
    // dataSource, dimensions & metrics are stored as canonical string values to decrease memory required for storing large numbers of segments.
    this.dataSource = dataSource;
    this.interval = interval;
    this.loadSpec = loadSpec;
    this.version = version;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.shardSpec = shardSpec;
    this.binaryVersion = binaryVersion;
    this.size = size;
    this.numRows = numRows;

    this.identifier = toSegmentId(
        this.dataSource,
        this.interval,
        this.version,
        shardSpec == null ? 0 : shardSpec.getPartitionNum()
    );
  }

  public DataSegment(
      String dataSource,
      Interval interval,
      String version,
      Map<String, Object> loadSpec,
      List<String> dimensions,
      List<String> metrics,
      ShardSpec shardSpec,
      Integer binaryVersion,
      long size
  )
  {
    this(dataSource, interval, version, loadSpec, dimensions, metrics, shardSpec, binaryVersion, size, -1);
  }

  /**
   * Get dataSource
   *
   * @return the dataSource
   */
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonSerialize(using = CommaListJoinSerializer.class)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @JsonIgnore
  public ShardSpec getShardSpecWithDefault()
  {
    return shardSpec == null ? NoneShardSpec.instance() : shardSpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getBinaryVersion()
  {
    return binaryVersion;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public String getIdentifier()
  {
    return identifier;
  }

  public SegmentDescriptor toDescriptor()
  {
    return new SegmentDescriptor(dataSource, interval, version, shardSpec == null ? 0 : shardSpec.getPartitionNum());
  }

  public DataSegment withDataSource(String dataSource)
  {
    return builder(this).dataSource(dataSource).build();
  }

  public DataSegment withInterval(Interval interval)
  {
    return builder(this).interval(interval).build();
  }

  public DataSegment withLoadSpec(Map<String, Object> loadSpec)
  {
    return builder(this).loadSpec(loadSpec).build();
  }

  public DataSegment withDimensions(List<String> dimensions)
  {
    return builder(this).dimensions(dimensions).build();
  }

  public DataSegment withMetrics(List<String> metrics)
  {
    return builder(this).metrics(metrics).build();
  }

  public DataSegment withSize(long size)
  {
    return builder(this).size(size).build();
  }

  public DataSegment withNumRows(int numRows)
  {
    this.numRows = numRows;
    return this;
  }

  public DataSegment withVersion(String version)
  {
    return builder(this).version(version).build();
  }

  public DataSegment withBinaryVersion(int binaryVersion)
  {
    return builder(this).binaryVersion(binaryVersion).build();
  }

  public DataSegment withShardSpec(ShardSpec shardSpec)
  {
    return builder(this).shardSpec(shardSpec).build();
  }

  public DataSegment withMinimum()
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        shardSpec,
        binaryVersion,
        size,
        numRows
    );
  }

  @Override
  public int compareTo(DataSegment dataSegment)
  {
    return getIdentifier().compareTo(dataSegment.getIdentifier());
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegment) {
      return getIdentifier().equals(((DataSegment) o).getIdentifier());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return getIdentifier().hashCode();
  }

  @Override
  public String toString()
  {
    return "DataSegment{" +
           "size=" + size +
           ", shardSpec=" + shardSpec +
           ", metrics=" + metrics +
           ", dimensions=" + dimensions +
           ", version='" + version + '\'' +
           ", loadSpec=" + loadSpec +
           ", interval=" + interval +
           ", dataSource='" + dataSource + '\'' +
           ", binaryVersion='" + binaryVersion + '\'' +
           ", size=" + size +
           ", numRows=" + numRows +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(DataSegment segment)
  {
    return new Builder(segment);
  }

  public static class Builder
  {
    private String dataSource;
    private Interval interval;
    private String version;
    private Map<String, Object> loadSpec;
    private List<String> dimensions;
    private List<String> metrics;
    private ShardSpec shardSpec;
    private Integer binaryVersion;
    private long size;
    private int numRows;

    public Builder()
    {
      this.loadSpec = ImmutableMap.of();
      this.dimensions = ImmutableList.of();
      this.metrics = ImmutableList.of();
      this.shardSpec = NoneShardSpec.instance();
      this.size = -1;
      this.numRows = -1;
    }

    public Builder(DataSegment segment)
    {
      this.dataSource = segment.getDataSource();
      this.interval = segment.getInterval();
      this.version = segment.getVersion();
      this.loadSpec = segment.getLoadSpec();
      this.dimensions = segment.getDimensions();
      this.metrics = segment.getMetrics();
      this.shardSpec = segment.getShardSpecWithDefault();
      this.binaryVersion = segment.getBinaryVersion();
      this.size = segment.getSize();
      this.numRows = segment.getNumRows();
    }

    public Builder dataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder interval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public Builder version(String version)
    {
      this.version = version;
      return this;
    }

    public Builder loadSpec(Map<String, Object> loadSpec)
    {
      this.loadSpec = loadSpec;
      return this;
    }

    public Builder dimensions(List<String> dimensions)
    {
      this.dimensions = dimensions;
      return this;
    }

    public Builder metrics(List<String> metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder shardSpec(ShardSpec shardSpec)
    {
      this.shardSpec = shardSpec;
      return this;
    }

    public Builder binaryVersion(Integer binaryVersion)
    {
      this.binaryVersion = binaryVersion;
      return this;
    }

    public Builder size(long size)
    {
      this.size = size;
      return this;
    }

    public Builder numRows(int numRows)
    {
      this.numRows = numRows;
      return this;
    }

    public DataSegment build()
    {
      // Check stuff that goes into the identifier, at least.
      Preconditions.checkNotNull(dataSource, "dataSource");
      Preconditions.checkNotNull(interval, "interval");
      Preconditions.checkNotNull(version, "version");
      Preconditions.checkNotNull(shardSpec, "shardSpec");

      return new DataSegment(
          dataSource,
          interval,
          version,
          loadSpec,
          dimensions,
          metrics,
          shardSpec,
          binaryVersion,
          size,
          numRows
      );
    }
  }

  private static class DataSourceInterner
  {
    private static final int MAX_VARIETY = 12;

    private final Map<String, Intern> cache = Maps.newConcurrentMap();

    public Intern get(String dataSource)
    {
      return cache.computeIfAbsent(dataSource, DataSourceInterner::create);
    }

    private static List<String> split(String columns)
    {
      return ImmutableList.copyOf(columns.split(","));
    }

    private static Intern create(String ds)
    {
      return new Intern(ds);
    }

    private static class Intern
    {
      private final String dataSource;

      private final Map<String, List<String>> columns = new LinkedHashMap<String, List<String>>()
      {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<String>> eldest)
        {
          return size() > MAX_VARIETY;
        }
      };

      public synchronized List<String> intern(String columnList)
      {
        return Strings.isNullOrEmpty(columnList)
               ? ImmutableList.of()
               : columns.computeIfAbsent(columnList, DataSourceInterner::split);
      }

      private Intern(String dataSource)
      {
        this.dataSource = dataSource;
      }
    }
  }

  public static DataSegment asKey(String identifier)
  {
    return new DataSegment()
    {
      @Override
      public String getIdentifier()
      {
        return identifier;
      }
    };
  }
}
