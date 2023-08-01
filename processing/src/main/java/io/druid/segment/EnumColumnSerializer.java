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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.IntTagged;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.segment.bitmap.Bitmaps;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.IntWriter;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde.SerdeBuilder;
import io.druid.segment.serde.EnumColumnPartSerde;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class EnumColumnSerializer implements MetricColumnSerializer
{
  public static EnumColumnSerializer create(
      String metric,
      ValueDesc type,
      CompressionStrategy compression,
      BitmapSerdeFactory bitmap
  )
  {
    String[] description = TypeUtils.splitDescriptiveType(type);
    if (description == null) {
      throw new IAE("enum type [%s] should have descriptions", type);
    }
    StringBuilder normalized = new StringBuilder("enum(");
    List<IntTagged<String>> values = Lists.newArrayList();
    values.add(IntTagged.of(0, ""));
    for (int i = 1; i < description.length; i++) {
      if (i > 1) {
        normalized.append(',');
      }
      String trimed = description[i].trim();
      values.add(IntTagged.of(i, StringUtils.unquote(trimed)));
      normalized.append(trimed);
    }
    normalized.append(')');

    Collections.sort(values, (e1, e2) -> e1.value.compareTo(e2.value));

    Object2IntMap<String> enums = new Object2IntOpenHashMap<>();
    enums.defaultReturnValue(-1);
    for (int i = 0; i < values.size(); i++) {
      IntTagged<String> e = values.get(i);
      if (enums.put(e.value, i) >= 0) {
        throw new IAE("duplicated enum value [%s] in %s", e.value, type);
      }
    }
    return new EnumColumnSerializer(
        ValueDesc.of(normalized.toString()),
        metric,
        enums,
        compression == null ? CompressionStrategy.NONE : compression,
        bitmap
    );
  }

  private final ValueDesc type;
  private final String metric;
  private final CompressionStrategy compression;
  private final BitmapSerdeFactory factory;

  private final Object2IntMap<String> enums;
  private final MutableBitmap[] mutables;

  private IntWriter values;

  public EnumColumnSerializer(
      ValueDesc type,
      String metric,
      Object2IntMap<String> enums,
      CompressionStrategy compression,
      BitmapSerdeFactory factory
  )
  {
    this.type = type;
    this.metric = metric;
    this.enums = enums;
    this.mutables = new MutableBitmap[enums.size()];
    this.compression = compression;
    this.factory = factory;
    for (int i = 0; i < mutables.length; i++) {
      mutables[i] = factory.getBitmapFactory().makeEmptyMutableBitmap();
    }
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    values = IntWriter.create(ioPeon, metric, enums.size(), compression);
    values.open();
  }

  private String suffix(String suffix)
  {
    return metric + suffix;
  }

  private int register(Object value)
  {
    int ix = enums.getOrDefault(StringUtils.toString(value, ""), -1);
    Preconditions.checkArgument(ix >= 0, "invalid value '%s'", value);
    return ix;
  }

  @Override
  public void serialize(int rowNum, Object aggs) throws IOException
  {
    int ix = register(aggs);
    values.add(ix);
    mutables[ix].add(rowNum);
  }

  @Override
  public void close() throws IOException
  {
    enums.clear();
    values.close();
  }

  @Override
  public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder) throws IOException
  {
    SerdeBuilder serde = DictionaryEncodedColumnPartSerde.builder();
    serde.withValue(values, false);
    serde.withBitmapIndex(Bitmaps.serialize(ioPeon, suffix(".bitmaps"), factory, mutables));

    ColumnDescriptor.Builder encoded = new ColumnDescriptor.Builder();
    encoded.setValueType(type);
    encoded.addSerde(serde.build(factory));

    builder.setValueType(ValueDesc.STRING);
    builder.addSerde(EnumColumnPartSerde.create(type, encoded.build()));
    return builder;
  }
}
