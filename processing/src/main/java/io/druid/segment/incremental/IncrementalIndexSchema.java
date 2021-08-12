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

package io.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.column.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class IncrementalIndexSchema
{
  public static IncrementalIndexSchema from(RowSignature signature)
  {
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    return from(signature, ImmutableMap.of());
  }

  public static IncrementalIndexSchema from(RowSignature signature, Map<String, String> mapping)
  {
    List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
    for (String dimension : signature.getDimensionNames()) {
      String outputName = mapping.getOrDefault(dimension, dimension);
      dimensionSchemas.add(DimensionSchema.of(outputName, dimension, ValueType.STRING));
    }
    boolean stringAsDimension = dimensionSchemas.isEmpty();
    List<AggregatorFactory> merics = Lists.newArrayList();
    for (Pair<String, ValueDesc> pair : signature.metricAndTypes()) {
      String metricName = pair.lhs;
      String outputName = mapping.getOrDefault(metricName, metricName);
      if (!outputName.equals(Column.TIME_COLUMN_NAME)) {
        if (stringAsDimension && (pair.rhs.isString() || pair.rhs.isMultiValued())) {
          dimensionSchemas.add(DimensionSchema.of(outputName, metricName, ValueType.STRING));
        } else {
          merics.add(new RelayAggregatorFactory(outputName, metricName, pair.rhs.typeName()));
        }
      }
    }
    return new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(Granularities.ALL)
        .withDimensionsSpec(new DimensionsSpec(dimensionSchemas, null, null))
        .withMetrics(merics)
        .withDimensionFixed(true)
        .withRollup(false)
        .build();
  }

  private final long minTimestamp;
  private final Granularity gran;
  private final Granularity segmentGran;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;
  private final boolean rollup;
  private final boolean dimensionFixed;
  private final boolean noQuery;
  private final Map<String, Map<String, String>> columnDescriptors;

  @JsonCreator
  public IncrementalIndexSchema(
      @JsonProperty("minTimestamp") long minTimestamp,
      @JsonProperty("gran") Granularity gran,
      @JsonProperty("segmentGran") Granularity segmentGran,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metrics") AggregatorFactory[] metrics,
      @JsonProperty("rollup") boolean rollup,
      @JsonProperty("dimensionFixed") boolean dimensionFixed,
      @JsonProperty("noQuery") boolean noQuery,
      @JsonProperty("columnDescriptors") Map<String, Map<String, String>> columnDescriptors
  )
  {
    this.minTimestamp = minTimestamp;
    this.gran = gran == null ? Granularities.NONE : gran;
    this.segmentGran = segmentGran;
    this.dimensionsSpec = dimensionsSpec;
    this.metrics = metrics == null ? new AggregatorFactory[0] : metrics;
    this.rollup = rollup;
    this.dimensionFixed = dimensionFixed;
    this.noQuery = noQuery;
    this.columnDescriptors = columnDescriptors;
  }

  @JsonProperty
  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  @JsonProperty
  public Granularity getGran()
  {
    return gran;
  }

  @JsonProperty
  public Granularity getSegmentGran()
  {
    return segmentGran;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public boolean isRollup()
  {
    return rollup;
  }

  @JsonProperty
  public boolean isDimensionFixed()
  {
    return dimensionFixed;
  }

  @JsonProperty
  public boolean isNoQuery()
  {
    return noQuery;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Map<String, String>> getColumnDescriptors()
  {
    return columnDescriptors;
  }

  public List<String> getDimensionNames()
  {
    return dimensionsSpec.getDimensionNames();
  }

  public List<String> getMetricNames()
  {
    return AggregatorFactory.toNames(Arrays.asList(metrics));
  }

  public List<String> getMetricNameTypes()
  {
    List<String> metricTypes = Lists.newArrayListWithCapacity(metrics.length);
    for (AggregatorFactory aggregatorFactory : metrics) {
      metricTypes.add(aggregatorFactory.getName() + ":" + aggregatorFactory.getOutputType());
    }
    return metricTypes;
  }

  public IncrementalIndexSchema withRollup(boolean rollup)
  {
    return new IncrementalIndexSchema(
        minTimestamp,
        gran,
        segmentGran,
        dimensionsSpec,
        metrics,
        rollup,
        dimensionFixed,
        noQuery,
        columnDescriptors
    );
  }

  public IncrementalIndexSchema withDimensionsSpec(DimensionsSpec dimensionsSpec)
  {
    return new IncrementalIndexSchema(
        minTimestamp,
        gran,
        segmentGran,
        dimensionsSpec,
        metrics,
        rollup,
        dimensionFixed,
        noQuery,
        columnDescriptors
    );
  }

  public IncrementalIndexSchema withMetrics(AggregatorFactory... metrics)
  {
    return new IncrementalIndexSchema(
        minTimestamp,
        gran,
        segmentGran,
        dimensionsSpec,
        metrics,
        rollup,
        dimensionFixed,
        noQuery,
        columnDescriptors
    );
  }

  public IncrementalIndexSchema withMinTimestamp(long minTimestamp)
  {
    return new IncrementalIndexSchema(
        minTimestamp,
        gran,
        segmentGran,
        dimensionsSpec,
        metrics,
        rollup,
        dimensionFixed,
        noQuery,
        columnDescriptors
    );
  }

  @Override
  public String toString()
  {
    return "IncrementalIndexSchema{" +
           "minTimestamp=" + minTimestamp +
           ", gran=" + gran +
           ", segmentGran=" + segmentGran +
           ", dimensionsSpec=" + dimensionsSpec +
           ", metrics=" + Arrays.toString(metrics) +
           ", rollup=" + rollup +
           ", dimensionFixed=" + dimensionFixed +
           '}';
  }

  public static class Builder
  {
    private long minTimestamp;
    private Granularity gran;
    private Granularity segmentGran;
    private DimensionsSpec dimensionsSpec;
    private AggregatorFactory[] metrics;
    private boolean dimensionFixed;
    private boolean rollup;
    private boolean noQuery;
    private Map<String, ValueDesc> knownTypes;
    private Map<String, Map<String, String>> columnDescs;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.gran = Granularities.NONE;
      this.dimensionsSpec = new DimensionsSpec(null, null, null);
      this.metrics = new AggregatorFactory[]{};
      this.rollup = true;
    }

    public Builder withMinTimestamp(long minTimestamp)
    {
      this.minTimestamp = minTimestamp;
      return this;
    }

    public Builder withQueryGranularity(Granularity gran)
    {
      this.gran = gran;
      return this;
    }

    public Builder withSegmentGranularity(Granularity segmentGran)
    {
      this.segmentGran = segmentGran;
      return this;
    }

    public Builder withDimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder withDimensionsSpec(InputRowParser<?> parser)
    {
      if (parser == null) {
        this.dimensionsSpec = DimensionsSpec.empty();
        this.knownTypes = null;
      } else {
        this.dimensionsSpec = parser.getDimensionsSpec();
        this.knownTypes = parser.knownTypes();
      }
      return this;
    }

    public Builder withDimensions(List<String> dimensions)
    {
      this.dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions), null, null);
      return this;
    }

    public Builder withDimensionAndTypes(Iterable<Pair<String, ValueDesc>> dimensionAndTypes)
    {
      List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
      for (Pair<String, ValueDesc> pair : dimensionAndTypes) {
        dimensionSchemas.add(DimensionSchema.of(pair.lhs, pair.rhs.unwrapDimension().type()));
      }
      this.dimensionsSpec = new DimensionsSpec(dimensionSchemas, null, null);
      return this;
    }

    public Builder withMetricAndTypes(Iterable<Pair<String, ValueDesc>> metricAndTypes)
    {
      return withMetrics(AggregatorFactory.toRelay(metricAndTypes));
    }

    public Builder withMetrics(List<AggregatorFactory> metrics)
    {
      if (!GuavaUtils.isNullOrEmpty(metrics) && !GuavaUtils.isNullOrEmpty(knownTypes)) {
        // hack
        final Supplier<TypeResolver> resolver = Suppliers.ofInstance(new TypeResolver.WithMap(knownTypes));
        final List<AggregatorFactory> resolved = Lists.newArrayList();
        for (AggregatorFactory metric : metrics) {
          resolved.add(metric.resolveIfNeeded(resolver));
        }
        this.metrics = resolved.toArray(new AggregatorFactory[0]);
      } else {
        this.metrics = metrics.toArray(new AggregatorFactory[0]);
      }
      return this;
    }

    public Builder withMetrics(AggregatorFactory... metrics)
    {
      return withMetrics(Arrays.asList(metrics));
    }

    public Builder withColumnDescs(Map<String, Map<String, String>> columnDescs)
    {
      this.columnDescs = columnDescs;
      return this;
    }

    public Builder withRollup(boolean rollup)
    {
      this.rollup = rollup;
      return this;
    }

    public Builder withDimensionFixed(boolean dimensionFixed)
    {
      this.dimensionFixed = dimensionFixed;
      return this;
    }

    public Builder withNoQuery(boolean noQuery)
    {
      this.noQuery = noQuery;
      return this;
    }

    public IncrementalIndexSchema build()
    {
      return new IncrementalIndexSchema(
          minTimestamp,
          gran,
          segmentGran,
          dimensionsSpec,
          metrics,
          rollup,
          dimensionFixed,
          noQuery,
          columnDescs
      );
    }
  }
}
