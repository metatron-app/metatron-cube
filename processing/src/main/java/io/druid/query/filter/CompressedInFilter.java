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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.extraction.ExtractionFn;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.Arrays;
import java.util.List;

@JsonTypeName("in.compressed")
public class CompressedInFilter extends DimFilter.FilterFactory implements DimFilter.Compressed
{
  private static final Logger LOG = new Logger(CompressedInFilter.class);

  private static final LZ4Compressor LZ4_COMP = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4FastDecompressor LZ4_DECOMP = LZ4Factory.fastestInstance().fastDecompressor();

  private static final int TRIVIAL_SIZE = 2048;

  public static DimFilter build(InDimFilter filter)
  {
    final List<String> values = filter.getValues();
    if (values.size() < TRIVIAL_SIZE) {
      return filter;
    }
    final long start = System.currentTimeMillis();
    final BytesOutputStream output = new BytesOutputStream(8192);
    for (String value : values) {
      output.writeVarSizeBytes(StringUtils.toUtf8WithNullToEmpty(value));
    }
    final BytesRef ref = output.asRef();
    final byte[] compressing = new byte[LZ4_COMP.maxCompressedLength(ref.length)];
    final int compressed = LZ4_COMP.compress(ref.bytes, 0, ref.length, compressing, 0);
    final int reduction = 100 * (ref.length - compressed) / ref.length;
    LOG.debug(
        "-- compressed in filter [%s], %,d bytes into %,d bytes (%d%% reduction, %d msec)",
        filter.getDimension(), ref.length, compressed, reduction, System.currentTimeMillis() - start
    );
    return new CompressedInFilter(
        filter.getDimension(),
        ref.length,
        values.size(),
        Arrays.copyOf(compressing, compressed),
        filter.getExtractionFn(),
        filter.getHash()
    );
  }

  private final String dimension;
  private final ExtractionFn extractionFn;
  private final int destLen;
  private final int valueLen;
  private final byte[] values;
  private final byte[] hash;

  @JsonCreator
  public CompressedInFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("destLen") int destLen,
      @JsonProperty("valueLen") int valueLen,
      @JsonProperty("values") byte[] values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JsonProperty("hash") byte[] hash
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    this.destLen = destLen;
    this.valueLen = valueLen;
    this.values = values;
    this.extractionFn = extractionFn;
    this.hash = hash;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public int getDestLen()
  {
    return destLen;
  }

  @JsonProperty
  public int getValueLen()
  {
    return valueLen;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getValues()
  {
    return values;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getHash()
  {
    return hash;
  }

  @Override
  public DimFilter forLog()
  {
    return new CompressedInFilter(dimension, destLen, valueLen, null, extractionFn, hash);
  }

  @Override
  public DimFilter decompress(Query parent)
  {
    final byte[] decompressed = LZ4_DECOMP.decompress(values, destLen);
    return new DecompressedInFilter(dimension, valueLen, decompressed, extractionFn, hash);
  }

  @Override
  public String toString()
  {
    return "CompressedInFilter{" +
           "dimension='" + dimension + '\'' +
           (extractionFn == null ? "" : ", extractionFn=" + extractionFn) +
           (values == null ? "" : ", sourceLen=" + values.length) +
           ", destLen=" + destLen +
           '}';
  }
}
