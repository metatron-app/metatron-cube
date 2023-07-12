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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.serde.ColumnPartSerde;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 *
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  private static final Logger LOG = new Logger(IndexSpec.class);

  public static final CompressionStrategy DEFAULT_COMPRESSION = CompressionStrategy.LZ4;

  public static final Set<String> COMPRESSION_NAMES = Arrays.stream(CompressionStrategy.values())
                                                             .map(x -> x.name().toLowerCase())
                                                             .collect(Collectors.toSet());

  public static final IndexSpec DEFAULT = new IndexSpec();

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final String dimensionCompression;
  private final String metricCompression;
  private final Map<String, SecondaryIndexingSpec> secondaryIndexing;
  private final Map<String, String> columnCompression;
  private final boolean allowNullForNumbers;
  private final Map<String, Float> expectedFSTReductions;

  private final List<CuboidSpec> cuboidSpecs;

  /**
   * Creates an IndexSpec with default parameters
   */
  public IndexSpec()
  {
    this(null, null, null, null, null, null, true, null);
  }

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   * @param bitmapSerdeFactory type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                           Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                           setting, or, if none was set, uses the default @{link BitmapSerde.DefaultBitmapSerdeFactory}
   *
   * @param dimensionCompression compression format for dimension columns, null to use the default
   *                             Defaults to @{link CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY}
   * @param metricCompression compression format for metric columns, null to use the default.
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") String dimensionCompression,
      @JsonProperty("metricCompression") String metricCompression,
      @JsonProperty("columnCompression") Map<String, String> columnCompression,
      @JsonProperty("secondaryIndexing") Map<String, SecondaryIndexingSpec> secondaryIndexing,
      @JsonProperty("cuboidSpecs") List<CuboidSpec> cuboidSpecs,
      @JsonProperty("allowNullForNumbers") boolean allowNullForNumbers,
      @JsonProperty("expectedFSTReductions") Map<String, Float> expectedFSTReductions
  )
  {
    Preconditions.checkArgument(
        dimensionCompression == null || COMPRESSION_NAMES.contains(dimensionCompression.toLowerCase()),
        "Unknown dimension compression type[%s]", dimensionCompression
    );

    Preconditions.checkArgument(
        metricCompression == null || COMPRESSION_NAMES.contains(metricCompression.toLowerCase()),
        "Unknown metric compression type[%s]", metricCompression
    );

    this.bitmapSerdeFactory = bitmapSerdeFactory != null ? bitmapSerdeFactory : new RoaringBitmapSerdeFactory();
    this.dimensionCompression = dimensionCompression == null ? null : dimensionCompression.toLowerCase();
    this.metricCompression = metricCompression == null ? null : metricCompression.toLowerCase();
    this.secondaryIndexing = secondaryIndexing;
    this.columnCompression = columnCompression;
    this.cuboidSpecs = cuboidSpecs;
    this.allowNullForNumbers = allowNullForNumbers;
    this.expectedFSTReductions = expectedFSTReductions;
  }

  public IndexSpec(
      BitmapSerdeFactory bitmapSerdeFactory,
      String dimensionCompression,
      String metricCompression
  )
  {
    this(bitmapSerdeFactory, dimensionCompression, metricCompression, null, null, null, false, null);
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

  @JsonProperty("expectedFSTReductions")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Float> getExpectedFSTReductions()
  {
    return expectedFSTReductions;
  }

  public SecondaryIndexingSpec getSecondaryIndexingSpec(String column)
  {
    return secondaryIndexing == null ? null : secondaryIndexing.get(column);
  }

  public DictionaryPartBuilder getFSTBuilder(String column, ObjectMapper mapper)
  {
    Float fstReduction = expectedFSTReductions == null ? null : expectedFSTReductions.get(column);
    if (fstReduction != null && fstReduction > 0) {
      for (String type : new String[]{"lucene8.fst", "lucene7.fst"}) {
        DictionaryPartBuilder builder = _makeBuilder(mapper, fstReduction.floatValue(), type);
        if (builder != null) {
          return builder;
        }
      }
    }
    return null;
  }

  private static DictionaryPartBuilder _makeBuilder(ObjectMapper mapper, float reduction, String type)
  {
    Map<String, Object> value = ImmutableMap.of("type", type, "reduction", reduction);
    try {
      ColumnPartSerde serDe = mapper.convertValue(value, ColumnPartSerde.class);
      if (serDe instanceof DictionaryPartBuilder) {
        return (DictionaryPartBuilder) serDe;
      }
    }
    catch (Exception e) {
      LOG.info("Failed to convert %s to fst builder (missing lucene extension?)", value);
    }
    return null;
  }

  public CompressionStrategy getCompressionStrategy(String column)
  {
    String strategy = columnCompression == null ? null : columnCompression.get(column);
    return strategy == null ? null : CompressionStrategy.valueOf(strategy.toUpperCase());
  }

  public CompressionStrategy getMetricCompressionStrategy()
  {
    return compressionStrategyForName(metricCompression);
  }

  // compression on row ids, not on dictionary
  public CompressionStrategy getDimensionCompressionStrategy()
  {
    return compressionStrategyForName(dimensionCompression);
  }

  private static CompressionStrategy compressionStrategyForName(String compression)
  {
    return compression == null ? DEFAULT_COMPRESSION : CompressionStrategy.valueOf(compression.toUpperCase());
  }

  public Map<String, Map<String, String>> getColumnDescriptors()
  {
    if (GuavaUtils.isNullOrEmpty(secondaryIndexing)) {
      return null;
    }
    Map<String, Map<String, String>> columnDescs = Maps.newHashMap();
    for (Map.Entry<String, SecondaryIndexingSpec> entry : secondaryIndexing.entrySet()) {
      if (entry.getValue() instanceof SecondaryIndexingSpec.WithDescriptor) {
        String columnName = entry.getKey();
        SecondaryIndexingSpec.WithDescriptor provider = (SecondaryIndexingSpec.WithDescriptor) entry.getValue();
        Map<String, String> columnDesc = provider.descriptor(columnName);
        if (!GuavaUtils.isNullOrEmpty(columnDesc)) {
          columnDescs.put(columnName, columnDesc);
        }
      }
    }
    return columnDescs;
  }

  @JsonIgnore
  public boolean needFinalizing()
  {
    return !GuavaUtils.isNullOrEmpty(secondaryIndexing) || !GuavaUtils.isNullOrEmpty(cuboidSpecs);
  }

  @JsonIgnore
  public IndexSpec asIntermediarySpec()
  {
    return new IndexSpec(
        bitmapSerdeFactory,
        dimensionCompression,
        metricCompression,
        columnCompression,
        null,
        null,
        allowNullForNumbers,
        needFinalizing() ? null : expectedFSTReductions    // seemed not much of overhead
    );
  }

  public IndexSpec withMetricCompression(String metricCompression)
  {
    return new IndexSpec(
        bitmapSerdeFactory,
        dimensionCompression,
        metricCompression,
        columnCompression,
        secondaryIndexing,
        cuboidSpecs,
        allowNullForNumbers,
        expectedFSTReductions
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
    if (!Objects.equals(expectedFSTReductions, indexSpec.expectedFSTReductions)) {
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
        metricCompression,
        secondaryIndexing,
        columnCompression,
        cuboidSpecs,
        allowNullForNumbers,
        expectedFSTReductions
    );
  }

  @Override
  public String toString()
  {
    return "IndexSpec{" +
           "allowNullForNumbers=" + allowNullForNumbers +
           (dimensionCompression == null ? "" : ", dimensionCompression='" + dimensionCompression + '\'') +
           (metricCompression == null ? "" : ", metricCompression='" + metricCompression + '\'') +
           (GuavaUtils.isNullOrEmpty(columnCompression) ? "" : ", columnCompression=" + columnCompression) +
           (GuavaUtils.isNullOrEmpty(secondaryIndexing) ? "" : ", secondaryIndexing=" + secondaryIndexing) +
           (GuavaUtils.isNullOrEmpty(expectedFSTReductions) ? "" : ", expectedFSTReductions=" + expectedFSTReductions) +
           (GuavaUtils.isNullOrEmpty(cuboidSpecs) ? "" : ", cuboidSpecs=" + cuboidSpecs) +
           '}';
  }
}
