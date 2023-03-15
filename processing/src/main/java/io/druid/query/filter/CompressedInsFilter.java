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
import com.google.common.collect.Lists;
import io.druid.common.guava.BytesRef;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.Arrays;
import java.util.List;

@JsonTypeName("ins.compressed")
public class CompressedInsFilter extends DimFilter.FilterFactory implements DimFilter.Compressed
{
  private static final Logger LOG = new Logger(CompressedInsFilter.class);

  private static final LZ4Compressor LZ4_COMP = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4FastDecompressor LZ4_DECOMP = LZ4Factory.fastestInstance().fastDecompressor();

  public static DimFilter build(InDimsFilter filter)
  {
    final List<List<String>> values = filter.getValues();
    if (values.get(0).size() < CompressedInFilter.TRIVIAL_SIZE) {
      return filter;
    }
    final long start = System.currentTimeMillis();
    final int[] destLens = new int[values.size()];
    final byte[][] bytes = new byte[values.size()][];
    final BytesOutputStream output = new BytesOutputStream(8192);
    for (int i = 0; i < destLens.length; i++) {
      output.clear();
      if (i == 0) {
        CompressedInFilter.encode(values.get(i), output);
      } else {
        output.writeVarSizeUTFs(values.get(i));
      }
      final BytesRef ref = output.asRef();
      final byte[] compressing = new byte[LZ4_COMP.maxCompressedLength(ref.length)];
      final int compressed = LZ4_COMP.compress(ref.bytes, 0, ref.length, compressing, 0);
      final int reduction = 100 * (ref.length - compressed) / ref.length;
      LOG.debug(
          "-- compressed ins filter %s[%d], %,d bytes into %,d bytes (%d%% reduction, %d msec)",
          filter.getDimensions(), i, ref.length, compressed, reduction, System.currentTimeMillis() - start
      );
      destLens[i] = ref.length;
      bytes[i] = Arrays.copyOf(compressing, compressed);
    }
    return new CompressedInsFilter(filter.getDimensions(), destLens, values.get(0).size(), bytes, filter.getHash());
  }

  private final List<String> dimensions;
  private final int[] destLens;
  private final int valueLen;
  private final byte[][] values;
  private final byte[] hash;

  @JsonCreator
  public CompressedInsFilter(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("destLens") int[] destLens,
      @JsonProperty("valueLen") int valueLen,
      @JsonProperty("values") byte[][] values,
      @JsonProperty("hash") byte[] hash
  )
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "dimension can not be null");
    this.destLens = destLens;
    this.valueLen = valueLen;
    this.values = values;
    this.hash = hash;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public int[] getDestLens()
  {
    return destLens;
  }

  @JsonProperty
  public int getValueLen()
  {
    return valueLen;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[][] getValues()
  {
    return values;
  }

  @Override
  public DimFilter forLog()
  {
    return new CompressedInsFilter(dimensions, destLens, valueLen, null, null);
  }

  @Override
  public DimFilter decompress(Query parent)
  {
    final List<List<String>> list = Lists.newArrayListWithCapacity(destLens.length);
    for (int i = 0; i < destLens.length; i++) {
      final byte[] decompressed = LZ4_DECOMP.decompress(values[i], destLens[i]);
      if (i == 0) {
        list.add(CompressedInFilter.decode(decompressed, valueLen, b -> StringUtils.fromUtf8(b), String.class));
      } else {
        list.add(new BytesInputStream(decompressed).readVarSizeUTFs(valueLen));
      }
    }
    return new InDimsFilter(dimensions, list, hash);
  }

  @Override
  public String toString()
  {
    int[] lengths = Arrays.stream(values).mapToInt(value -> value.length).toArray();
    return "CompressedInFilter{" +
           "dimensions=" + dimensions +
           ", sourceLens=" + Arrays.toString(lengths) +
           ", destLens=" + Arrays.toString(destLens) +
           '}';
  }
}
