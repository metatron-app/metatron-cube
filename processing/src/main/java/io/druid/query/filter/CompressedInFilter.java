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
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.extraction.ExtractionFn;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@JsonTypeName("in.compressed")
public class CompressedInFilter extends DimFilter.FilterFactory implements DimFilter.Compressed
{
  private static final Logger LOG = new Logger(CompressedInFilter.class);

  private static final LZ4Compressor LZ4_COMP = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4FastDecompressor LZ4_DECOMP = LZ4Factory.fastestInstance().fastDecompressor();

  private static final int TRIVIAL_SIZE = 1024;

  public static DimFilter build(InDimFilter filter)
  {
    final List<String> values = filter.getValues();
    if (values.size() < TRIVIAL_SIZE) {
      return filter;
    }
    final long start = System.currentTimeMillis();
    final BytesRef ref = encode(values, new BytesOutputStream(8192));
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

  static BytesRef encode(List<String> values, BytesOutputStream output)
  {
    byte[] prev = StringUtils.toUtf8WithNullToEmpty(values.get(0));
    output.writeVarSizeBytes(prev);
    for (int i = 1; i < values.size(); i++) {
      byte[] current = StringUtils.toUtf8WithNullToEmpty(values.get(i));
      int common = commonPrefix(prev, current);
      output.writeUnsignedVarInt(common);
      output.writeUnsignedVarInt(current.length - common);
      output.write(current, common, current.length - common);
      prev = current;
    }
    return output.asRef();
  }

  static List<BinaryRef> decode(byte[] bytes, int valueLen)
  {
    return decode(bytes, valueLen, BytesRef::of, BinaryRef.class);
  }

  @SuppressWarnings("unchecked")
  static <T> List<T> decode(byte[] bytes, int valueLen, Function<byte[], T> converter, Class<T> clazz)
  {
    T[] refs = (T[]) Array.newInstance(clazz, valueLen);
    BytesInputStream input = new BytesInputStream(bytes);

    byte[] prev = input.readVarSizeBytes();
    refs[0] = converter.apply(prev);
    for (int i = 1; i < valueLen; i++) {
      int common = input.readUnsignedVarInt();
      int remains = input.readUnsignedVarInt();
      byte[] value = new byte[common + remains];
      if (common > 0) {
        System.arraycopy(prev, 0, value, 0, common);
      }
      input.readAssert(value, common, remains);
      refs[i] = converter.apply(value);
      prev = value;
    }
    return Arrays.asList(refs);
  }

  private static final int MAX_DEPTH = 4;

  static List<BinaryRef> _decode(byte[] bytes, int valueLen)
  {
    List<BinaryRef> refs = Lists.newArrayListWithCapacity(valueLen);
    BytesInputStream input = new BytesInputStream(bytes);

    int depth = 0;
    int prevCommon = 0;
    BinaryRef prev = input.readVarSizeRef();
    refs.add(prev);
    for (int i = 1; i < valueLen; i++) {
      int common = input.readUnsignedVarInt();
      BinaryRef remains = input.readVarSizeRef();
      BinaryRef current;
      if (common == 0) {
        current = remains;
        depth = 0;
      } else {
        if (common <= prevCommon) {
          prev = ((Decoded) prev).prev;
        } else if (++depth >= MAX_DEPTH) {
          prev = BytesRef.of(((Decoded) prev).toBytes());
          depth = 1;
        }
        current = new Decoded(common, common + remains.length(), prev, remains);
      }
      prevCommon = common;
      prev = current;
      refs.add(current);
    }
    return refs;
  }

  private static int commonPrefix(byte[] b1, byte[] b2)
  {
    final int limit = Math.min(b1.length, b2.length);
    for (int i = 0; i < limit; i++) {
      if (b1[i] != b2[i]) {
        return i;
      }
    }
    return limit;
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

  private static final class Decoded implements BinaryRef
  {
    private final int common;
    private final int length;
    private final BinaryRef prev;
    private final BinaryRef remains;

    private Decoded(int common, int length, BinaryRef prev, BinaryRef remains)
    {
      Preconditions.checkArgument(prev != remains);
      this.common = common;
      this.length = length;
      this.prev = prev;
      this.remains = remains;
    }

    @Override
    public int length()
    {
      return length;
    }

    @Override
    public byte get(int index)
    {
      return index < common ? prev.get(index) : remains.get(index - common);
    }

    @Override
    public ByteBuffer toBuffer()
    {
      return ByteBuffer.wrap(toBytes());
    }

    @Override
    public byte[] toBytes()
    {
      byte[] bytes = new byte[length];
      int i = 0;
      for (; i < common; i++) {
        bytes[i] = prev.get(i);
      }
      for (; i < length; i++) {
        bytes[i] = remains.get(i - common);
      }
      return bytes;
    }
  }
}
