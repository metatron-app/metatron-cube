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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.select.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class IncrementalIndexSchema
{
  private final long minTimestamp;
  private final Granularity gran;
  private final Granularity segmentGran;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;
  private final boolean rollup;
  private final boolean fixedSchema;
  private final boolean noQuery;

  @JsonCreator
  public IncrementalIndexSchema(
      @JsonProperty("minTimestamp") long minTimestamp,
      @JsonProperty("gran") Granularity gran,
      @JsonProperty("segmentGran") Granularity segmentGran,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metrics") AggregatorFactory[] metrics,
      @JsonProperty("rollup") boolean rollup,
      @JsonProperty("fixedSchema") boolean fixedSchema,
      @JsonProperty("noQuery") boolean noQuery
  )
  {
    this.minTimestamp = minTimestamp;
    this.gran = gran == null ? Granularities.NONE : gran;
    this.segmentGran = segmentGran;
    this.dimensionsSpec = dimensionsSpec;
    this.metrics = metrics == null ? new AggregatorFactory[0] : metrics;
    this.rollup = rollup;
    this.fixedSchema = fixedSchema;
    this.noQuery = noQuery;
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
  public boolean isFixedSchema()
  {
    return fixedSchema;
  }

  @JsonProperty
  public boolean isNoQuery()
  {
    return noQuery;
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

  public Schema asSchema(final boolean keepDimensionMark)
  {
    List<ValueDesc> types = Lists.newArrayList();
    for (DimensionSchema schema : dimensionsSpec.getDimensions()) {
      if (keepDimensionMark) {
        types.add(ValueDesc.ofDimension(ValueType.of(schema.getTypeName())));
      } else {
        types.add(ValueDesc.of(schema.getTypeName()));
      }
    }
    for (AggregatorFactory aggregatorFactory : metrics) {
      types.add(aggregatorFactory.getOutputType());
    }
    List<String> dimensions = dimensionsSpec.getDimensionNames();
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.asMap(metrics);
    return new Schema(dimensions, getMetricNames(), types, aggregators, null, null);
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
        fixedSchema,
        noQuery
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
        fixedSchema,
        noQuery
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
        fixedSchema,
        noQuery
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
        fixedSchema,
        noQuery
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
           ", fixedSchema=" + fixedSchema +
           '}';
  }

  public static class Builder
  {
    private long minTimestamp;
    private Granularity gran;
    private Granularity segmentGran;
    private DimensionsSpec dimensionsSpec;
    private AggregatorFactory[] metrics;
    private boolean fixedSchema;
    private boolean rollup;
    private boolean noQuery;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.gran = QueryGranularities.NONE;
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

    public Builder withDimensionsSpec(InputRowParser parser)
    {
      this.dimensionsSpec = parser == null ? new DimensionsSpec(null, null, null) : parser.getDimensionsSpec();
      return this;
    }

    public Builder withDimensions(List<String> dimensions)
    {
      this.dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions), null, null);
      return this;
    }

    public Builder withDimensions(List<String> dimensions, List<ValueDesc> dimensionTypes)
    {
      List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
      for (int i = 0; i < dimensions.size(); i++) {
        ValueDesc valueDesc = dimensionTypes.get(i);
        dimensionSchemas.add(
            DimensionSchema.of(
                dimensions.get(i),
                valueDesc.isStringOrDimension() ? ValueType.STRING : valueDesc.type()
            )
        );
      }
      this.dimensionsSpec = new DimensionsSpec(dimensionSchemas, null, null);
      return this;
    }

    public Builder withMetrics(List<String> columns, List<ValueDesc> type)
    {
      return withMetrics(AggregatorFactory.toRelay(columns, type));
    }

    public Builder withMetrics(List<AggregatorFactory> metrics)
    {
      this.metrics = metrics.toArray(new AggregatorFactory[0]);
      return this;
    }

    public Builder withMetrics(AggregatorFactory... metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder withRollup(boolean rollup)
    {
      this.rollup = rollup;
      return this;
    }

    public Builder withFixedSchema(boolean fixedSchema)
    {
      this.fixedSchema = fixedSchema;
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
          fixedSchema,
          noQuery
      );
    }
  }
}
