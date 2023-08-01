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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.segment.bitmap.Bitmaps;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.IntWriter;
import io.druid.segment.data.IntsWriter;
import io.druid.segment.data.VintValues;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde.SerdeBuilder;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;

public class TagColumnSerializer implements MetricColumnSerializer
{
  public static TagColumnSerializer create(
      String metric,
      ValueDesc type,
      CompressionStrategy compression,
      BitmapSerdeFactory bitmap
  )
  {
    int maxValue = UnsignedBytes.toInt(UnsignedBytes.MAX_VALUE);
    String[] description = TypeUtils.splitDescriptiveType(type);
    if (description != null && description.length > 0) {
      maxValue = Integer.parseInt(description[0]);
    }
    return new TagColumnSerializer(metric, maxValue, compression == null ? CompressionStrategy.NONE : compression, bitmap);
  }

  private final String metric;
  private final int maxValue;
  private final CompressionStrategy compression;
  private final BitmapSerdeFactory factory;

  private final Object2IntMap<String> hash = new Object2IntOpenHashMap<>();

  private IntWriter sizes;
  private IntWriter values;

  public TagColumnSerializer(String metric, int maxValue, CompressionStrategy compression, BitmapSerdeFactory factory)
  {
    this.metric = metric;
    this.maxValue = maxValue;
    this.compression = compression;
    this.factory = factory;
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    sizes = IntWriter.create(ioPeon, suffix(".s"), maxValue, CompressionStrategy.NONE);
    sizes.open();
    values = IntWriter.create(ioPeon, suffix(".v"), maxValue, CompressionStrategy.NONE);
    values.open();
  }

  private String suffix(String suffix)
  {
    return metric + suffix;
  }

  private int register(Object value)
  {
    return hash.computeIntIfAbsent(StringUtils.toString(value, ""), v -> hash.size());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void serialize(int rowNum, Object aggs) throws IOException
  {
    int size = 0;
    if (StringUtils.isNullOrEmpty(aggs)) {
      values.add(register(""));
      size++;
    } else if (aggs instanceof List) {
      List<Object> document = (List<Object>) aggs;
      for (int i = 0; i < document.size(); i++) {
        values.add(register(document.get(i)));
      }
      size += document.size();
    } else if (aggs.getClass().isArray()) {
      int length = Array.getLength(aggs);
      for (int i = 0; i < length; i++) {
        values.add(register(Array.get(aggs, i)));
      }
      size += length;
    } else if (aggs instanceof Iterable) {
      for (Object value : (Iterable) aggs) {
        values.add(register(value));
        size++;
      }
    } else {
      values.add(register(aggs));
      size++;
    }
    if (hash.size() > maxValue) {
      throw new ISE("Exceeding maxValue [%s]", maxValue);
    }
    sizes.add(size);
  }

  @Override
  public void close() throws IOException
  {
    sizes.close();
    values.close();
  }

  @Override
  public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder) throws IOException
  {
    List<Object2IntMap.Entry<String>> entries = Lists.newArrayList(hash.object2IntEntrySet());

    Collections.sort(entries, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));

    SerdeBuilder serde = DictionaryEncodedColumnPartSerde.builder();

    int[] mapping = new int[entries.size()];  // ix to id
    MutableBitmap[] mutables = new MutableBitmap[mapping.length];
    GenericIndexedWriter<String> dictionary = GenericIndexedWriter.forDictionaryV2(ioPeon, suffix(".dictionary"));
    dictionary.open();

    for (int i = 0; i < mapping.length; i++) {
      mutables[i] = factory.getBitmapFactory().makeEmptyMutableBitmap();
      Object2IntMap.Entry<String> e = entries.get(i);
      dictionary.add(e.getKey());
      mapping[e.getIntValue()] = i;
    }
    hash.clear();
    entries.clear();

    dictionary.close();
    serde.withDictionary(dictionary);

    byte numBytes = VintValues.getNumBytesForMax(maxValue);
    if (sizes.count() == values.count()) {
      // single-valued
      VintValues values = new VintValues(Files.map(ioPeon.getFile(suffix(".v.values"))), numBytes);
      IntWriter rewritten = IntWriter.create(ioPeon, metric, mapping.length, compression);
      rewritten.open();
      for (int id = 0; id < values.size(); id++) {
        int ix = values.get(id);
        rewritten.add(mapping[ix]);
        mutables[mapping[ix]].add(id);
      }
      rewritten.close();
      serde.withValue(rewritten, false);
    } else {
      // multi-valued
      VintValues sizes = new VintValues(Files.map(ioPeon.getFile(suffix(".s.values"))), numBytes);
      VintValues values = new VintValues(Files.map(ioPeon.getFile(suffix(".v.values"))), numBytes);
      IntsWriter rewritten = IntsWriter.create(ioPeon, metric, mapping.length, compression);
      rewritten.open();
      int offset = 0;
      for (int id = 0; id < sizes.size(); id++) {
        int[] ixs = new int[sizes.get(id)];
        for (int i = 0; i < ixs.length; i++) {
          ixs[i] = mapping[values.get(offset++)];
          mutables[ixs[i]].add(id);
        }
        rewritten.add(ixs);
      }
      rewritten.close();
      serde.withValue(rewritten, true);
    }
    serde.withBitmapIndex(Bitmaps.serialize(ioPeon, suffix(".bitmaps"), factory, mutables));

    builder.setValueType(ValueDesc.STRING);
    builder.addSerde(serde.build(factory));
    return builder;
  }
}
