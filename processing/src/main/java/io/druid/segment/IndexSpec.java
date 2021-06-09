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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.lucene.LuceneIndexingSpec;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 *
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  public static final IndexSpec DEFAULT = new IndexSpec();

  public static final String UNCOMPRESSED = "uncompressed";
  public static final CompressionStrategy DEFAULT_METRIC_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY;
  public static final String DEFAULT_DIMENSION_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY.name().toLowerCase();

  private static final Set<String> COMPRESSION_NAMES = Sets.newHashSet(
      Iterables.transform(
          Arrays.asList(CompressionStrategy.values()),
          new Function<CompressionStrategy, String>()
          {
            @Nullable
            @Override
            public String apply(CompressionStrategy input)
            {
              return input.name().toLowerCase();
            }
          }
      )
  );

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final String dimensionCompression;
  private final String metricCompression;
  private final Map<String, SecondaryIndexingSpec> secondaryIndexing;
  private final Map<String, String> columnCompression;
  private final ColumnIncluderator dimensionSketches;
  private final boolean allowNullForNumbers;

  private final List<CuboidSpec> cuboidSpecs;

  /**
   * Creates an IndexSpec with default parameters
   */
  public IndexSpec()
  {
    this(null, null, null, null, null, null, null, false);
  }

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   *
   * @param bitmapSerdeFactory type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                           Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                           setting, or, if none was set, uses the default @{link BitmapSerde.DefaultBitmapSerdeFactory}
   *
   * @param dimensionCompression compression format for dimension columns, null to use the default
   *                             Defaults to @{link CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY}
   *
   * @param metricCompression compression format for metric columns, null to use the default.
   *                          Defaults to @{link CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY}
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") String dimensionCompression,
      @JsonProperty("dimensionSketches") ColumnIncluderator dimensionSketches,
      @JsonProperty("metricCompression") String metricCompression,
      @JsonProperty("columnCompression") Map<String, String> columnCompression,
      @JsonProperty("secondaryIndexing") Map<String, SecondaryIndexingSpec> secondaryIndexing,
      @JsonProperty("cuboidSpecs") List<CuboidSpec> cuboidSpecs,
      @JsonProperty("allowNullForNumbers") boolean allowNullForNumbers
  )
  {
    Preconditions.checkArgument(dimensionCompression == null || dimensionCompression.equals(UNCOMPRESSED) || COMPRESSION_NAMES.contains(dimensionCompression),
                                "Unknown compression type[%s]", dimensionCompression);

    Preconditions.checkArgument(metricCompression == null || COMPRESSION_NAMES.contains(metricCompression),
                                "Unknown compression type[%s]", metricCompression);

    this.bitmapSerdeFactory = bitmapSerdeFactory != null ? bitmapSerdeFactory : new RoaringBitmapSerdeFactory();
    this.dimensionCompression = dimensionCompression;
    this.dimensionSketches = dimensionSketches;
    this.metricCompression = metricCompression;
    this.secondaryIndexing = secondaryIndexing;
    this.columnCompression = columnCompression;
    this.cuboidSpecs = cuboidSpecs;
    this.allowNullForNumbers = allowNullForNumbers;
  }

  public IndexSpec(
      BitmapSerdeFactory bitmapSerdeFactory,
      String dimensionCompression,
      String metricCompression
  )
  {
    this(bitmapSerdeFactory, dimensionCompression, null, metricCompression, null, null, null, false);
  }

  @JsonProperty("bitmap")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty("dimensionCompression")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty("metricCompression")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getMetricCompression()
  {
    return metricCompression;
  }

  @JsonProperty("dimensionSketches")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ColumnIncluderator getDimensionSketches()
  {
    return dimensionSketches;
  }

  @JsonProperty("secondaryIndexing")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, SecondaryIndexingSpec> getSecondaryIndexing()
  {
    return secondaryIndexing;
  }

  @JsonProperty("columnCompression")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getColumnCompression()
  {
    return columnCompression;
  }

  @JsonProperty("cuboidSpecs")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<CuboidSpec> getCuboidSpecs()
  {
    return cuboidSpecs;
  }

  @JsonProperty("allowNullForNumbers")
  public boolean isAllowNullForNumbers()
  {
    return allowNullForNumbers;
  }

  @JsonIgnore
  public boolean needFinalizing()
  {
    return !GuavaUtils.isNullOrEmpty(secondaryIndexing) || !GuavaUtils.isNullOrEmpty(cuboidSpecs);
  }

  public SecondaryIndexingSpec getSecondaryIndexingSpec(String column)
  {
    return secondaryIndexing == null ? null : secondaryIndexing.get(column);
  }

  public CompressionStrategy getCompressionStrategy(String column, CompressionStrategy defaultStrategy)
  {
    String strategy = columnCompression == null ? null : columnCompression.get(column);
    return strategy == null ? defaultStrategy : CompressionStrategy.valueOf(strategy.toUpperCase());
  }

  public CompressionStrategy getMetricCompressionStrategy()
  {
    return metricCompression == null ? DEFAULT_METRIC_COMPRESSION :
           CompressionStrategy.valueOf(metricCompression.toUpperCase());
  }

  // todo : it's compression on row ids, not on dictionary
  public CompressionStrategy getDimensionCompressionStrategy()
  {
    return dimensionCompression == null ?
           dimensionCompressionStrategyForName(DEFAULT_DIMENSION_COMPRESSION) :
           dimensionCompressionStrategyForName(dimensionCompression);
  }

  private static CompressionStrategy dimensionCompressionStrategyForName(String compression)
  {
    return compression.equals(UNCOMPRESSED) ? null : CompressionStrategy.valueOf(compression.toUpperCase());
  }

  public Map<String, Map<String, String>> getColumnDescriptors()
  {
    if (GuavaUtils.isNullOrEmpty(secondaryIndexing)) {
      return null;
    }
    Map<String, Map<String, String>> columnDescs = Maps.newHashMap();
    for (Map.Entry<String, SecondaryIndexingSpec> entry : secondaryIndexing.entrySet()) {
      if (entry.getValue() instanceof LuceneIndexingSpec) {
        String columnName = entry.getKey();
        LuceneIndexingSpec luceneSpec = (LuceneIndexingSpec) entry.getValue();
        Map<String, String> columnDesc = LuceneIndexingSpec.getFieldDescriptors(luceneSpec.getStrategies(columnName));
        if (!GuavaUtils.isNullOrEmpty(columnDesc)) {
          columnDescs.put(columnName, columnDesc);
        }
      }
    }
    return columnDescs;
  }

  public IndexSpec asIntermediarySpec()
  {
    return new IndexSpec(
        bitmapSerdeFactory,
        dimensionCompression,
        dimensionSketches,
        metricCompression,
        columnCompression,
        null,
        null,
        allowNullForNumbers
    );
  }

  public IndexSpec withMetricCompression(String metricCompression)
  {
    return new IndexSpec(
        bitmapSerdeFactory,
        dimensionCompression,
        dimensionSketches,
        metricCompression,
        columnCompression,
        secondaryIndexing,
        cuboidSpecs,
        allowNullForNumbers
    );
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

    IndexSpec indexSpec = (IndexSpec) o;

    if (!Objects.equals(bitmapSerdeFactory, indexSpec.bitmapSerdeFactory)) {
      return false;
    }
    if (!Objects.equals(dimensionCompression, indexSpec.dimensionCompression)) {
      return false;
    }
    if (!Objects.equals(dimensionSketches, indexSpec.dimensionSketches)) {
      return false;
    }
    if (!Objects.equals(metricCompression, indexSpec.metricCompression)) {
      return false;
    }
    if (!Objects.equals(secondaryIndexing, indexSpec.secondaryIndexing)) {
      return false;
    }
    if (!Objects.equals(columnCompression, indexSpec.columnCompression)) {
      return false;
    }
    if (!Objects.equals(cuboidSpecs, indexSpec.cuboidSpecs)) {
      return false;
    }
    if (allowNullForNumbers != indexSpec.allowNullForNumbers) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        bitmapSerdeFactory,
        dimensionCompression,
        dimensionSketches,
        metricCompression,
        secondaryIndexing,
        columnCompression,
        cuboidSpecs,
        allowNullForNumbers
    );
  }
}
