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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("segmentMetadata")
public class SegmentMetadataQuery extends BaseQuery<SegmentAnalysis> implements Query.VCSupport<SegmentAnalysis>
{
  public static SegmentMetadataQuery of(String dataSource, AnalysisType... analysisTypes)
  {
    return new SegmentMetadataQuery(
        TableDataSource.of(dataSource),
        QuerySegmentSpec.ETERNITY,
        null,
        null,
        null,
        null,
        of(analysisTypes),
        null,
        null,
        null
    );
  }

  public static EnumSet<AnalysisType> of(AnalysisType... analysisTypes)
  {
    if (analysisTypes.length == 0) {
      return EnumSet.noneOf(AnalysisType.class);
    }
    return EnumSet.of(analysisTypes[0], Arrays.copyOfRange(analysisTypes, 1, analysisTypes.length));
  }

  public enum AnalysisType
  {
    CARDINALITY,
    SERIALIZED_SIZE,
    INTERVAL,
    AGGREGATORS,
    MINMAX,
    NULL_COUNT,
    QUERYGRANULARITY,
    INGESTED_NUMROW,
    ROLLUP,
    LAST_ACCESS_TIME;

    @JsonValue
    @Override
    public String toString()
    {
      return this.name().toLowerCase();
    }

    @JsonCreator
    public static AnalysisType fromString(String name)
    {
      return valueOf(name.toUpperCase());
    }
  }

  public static final Interval DEFAULT_INTERVAL = new Interval(
      JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT
  );

  public static final EnumSet<AnalysisType> DEFAULT_NON_COLUMN_STATS = EnumSet.of(
      AnalysisType.SERIALIZED_SIZE,
      AnalysisType.LAST_ACCESS_TIME,
      AnalysisType.INGESTED_NUMROW,
      AnalysisType.INTERVAL
  );

  public static final EnumSet<AnalysisType> DEFAULT_ANALYSIS_TYPES = EnumSet.of(
      AnalysisType.CARDINALITY,
      AnalysisType.SERIALIZED_SIZE,
      AnalysisType.INTERVAL,
      AnalysisType.MINMAX
  );

  private final List<VirtualColumn> virtualColumns;
  private final ColumnIncluderator toInclude;
  private final List<String> columns;
  private final boolean merge;
  private final boolean usingDefaultInterval;
  private final EnumSet<AnalysisType> analysisTypes;
  private final boolean lenientAggregatorMerge;

  @JsonCreator
  public SegmentMetadataQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("toInclude") ColumnIncluderator toInclude,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("merge") Boolean merge,
      @JsonProperty("analysisTypes") EnumSet<AnalysisType> analysisTypes,
      @JsonProperty("usingDefaultInterval") Boolean useDefaultInterval,
      @JsonProperty("lenientAggregatorMerge") Boolean lenientAggregatorMerge,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.usingDefaultInterval = querySegmentSpec == null || useDefaultInterval != null && useDefaultInterval;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.toInclude = toInclude;
    this.columns = columns;
    this.merge = merge == null ? false : merge;
    this.analysisTypes = (analysisTypes == null) ? DEFAULT_ANALYSIS_TYPES : analysisTypes;
    Preconditions.checkArgument(
        dataSource instanceof TableDataSource,
        "SegmentMetadataQuery only supports table datasource"
    );
    this.lenientAggregatorMerge = lenientAggregatorMerge == null ? false : lenientAggregatorMerge;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public ColumnIncluderator getToInclude()
  {
    return toInclude;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  @JsonProperty
  public boolean isUsingDefaultInterval()
  {
    return usingDefaultInterval;
  }

  @Override
  public String getType()
  {
    return Query.SEGMENT_METADATA;
  }

  @JsonProperty
  public EnumSet<AnalysisType> getAnalysisTypes()
  {
    return analysisTypes;
  }

  @JsonProperty
  public boolean isLenientAggregatorMerge()
  {
    return lenientAggregatorMerge;
  }

  @Override
  public Comparator<SegmentAnalysis> getMergeOrdering(List<String> columns)
  {
    Comparator<SegmentAnalysis> ordering = GuavaUtils.<SegmentAnalysis>noNullableNatural();
    if (!merge && analyzingInterval()) {
      ordering = Ordering.from(JodaUtils.intervalsByStartThenEnd()).onResultOf(new Function<SegmentAnalysis, Interval>()
      {
        @Override
        public Interval apply(SegmentAnalysis input)
        {
          return JodaUtils.umbrellaInterval(input.getIntervals());
        }
      });
    }
    return isDescending() ? Comparators.REVERT(ordering) : ordering;
  }

  public boolean analyzingOnlyInterval()
  {
    return analysisTypes.size() == 1
           && analysisTypes.contains(AnalysisType.INTERVAL)
           && toInclude instanceof NoneColumnIncluderator;
  }

  public boolean analyzingInterval()
  {
    return analysisTypes.contains(AnalysisType.INTERVAL);
  }

  public boolean hasAggregators()
  {
    return analysisTypes.contains(AnalysisType.AGGREGATORS);
  }

  public boolean hasQueryGranularity()
  {
    return analysisTypes.contains(AnalysisType.QUERYGRANULARITY);
  }

  public boolean hasRollup()
  {
    return analysisTypes.contains(AnalysisType.ROLLUP);
  }

  public boolean hasMinMax()
  {
    return analysisTypes.contains(AnalysisType.MINMAX);
  }

  public boolean hasIngestedNumRows()
  {
    return analysisTypes.contains(AnalysisType.INGESTED_NUMROW);
  }

  public boolean hasSerializedSize()
  {
    return analysisTypes.contains(AnalysisType.SERIALIZED_SIZE);
  }

  public boolean hasLastAccessTime()
  {
    return analysisTypes.contains(AnalysisType.LAST_ACCESS_TIME);
  }

  @Override
  public SegmentMetadataQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        columns,
        merge,
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public SegmentMetadataQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        spec,
        virtualColumns,
        toInclude,
        columns,
        merge,
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge,
        getContext()
    );
  }

  @Override
  public SegmentMetadataQuery withDataSource(DataSource dataSource)
  {
    return new SegmentMetadataQuery(
        dataSource,
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        columns,
        merge,
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge,
        getContext()
    );
  }

  public SegmentMetadataQuery withColumns(ColumnIncluderator includerator)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        includerator,
        columns,
        merge,
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge,
        getContext()
    );
  }

  @Override
  public SegmentMetadataQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        columns,
        merge,
        analysisTypes,
        usingDefaultInterval,
        lenientAggregatorMerge,
        getContext()
    );
  }

  public SegmentMetadataQuery withMoreAnalysis(AnalysisType... moreAnalysis)
  {
    Set<AnalysisType> current = Sets.newHashSet(analysisTypes);
    current.addAll(Arrays.asList(moreAnalysis));
    EnumSet<AnalysisType> added = EnumSet.copyOf(current);
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        toInclude,
        columns,
        merge,
        added,
        usingDefaultInterval,
        lenientAggregatorMerge,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SegmentMetadataQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", virtualColumns=" + virtualColumns +
           ", toInclude=" + toInclude +
           ", columns=" + columns +
           ", merge=" + merge +
           ", usingDefaultInterval=" + usingDefaultInterval +
           ", analysisTypes=" + analysisTypes +
           ", lenientAggregatorMerge=" + lenientAggregatorMerge +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SegmentMetadataQuery that = (SegmentMetadataQuery) o;
    return merge == that.merge &&
           usingDefaultInterval == that.usingDefaultInterval &&
           lenientAggregatorMerge == that.lenientAggregatorMerge &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(toInclude, that.toInclude) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(analysisTypes, that.analysisTypes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        virtualColumns,
        toInclude,
        columns,
        merge,
        usingDefaultInterval,
        analysisTypes,
        lenientAggregatorMerge
    );
  }
}
