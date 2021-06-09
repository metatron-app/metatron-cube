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

package io.druid.segment.bitmap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.ToStringDictionary;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 */
public class BitSetInvertedIndexingSpec implements SecondaryIndexingSpec
{
  private static final ObjectStrategy<ImmutableBitmap> STRATEGY = RoaringBitmapSerdeFactory.objectStrategy;

  @Override
  public MetricColumnSerializer serializer(String columnName, final ValueDesc type)
  {
    if (!type.isBitSet()) {
      return MetricColumnSerializer.DUMMY;
    }
    return new MetricColumnSerializer()
    {
      private final List<BitSet> bitsets = Lists.newArrayList();
      private IOPeon ioPeon;

      @Override
      public void open(IOPeon ioPeon) throws IOException
      {
        this.ioPeon = ioPeon;
      }

      @Override
      public void serialize(int rowNum, Object obj) throws IOException
      {
        if (obj == null) {
          return;
        }
        final BitSet bitSet = (BitSet) obj;
        int x = bitSet.nextSetBit(0);
        for (; x >= 0; x = bitSet.nextSetBit(x + 1)) {
          setFor(x).set(rowNum, true);
        }
      }

      private BitSet setFor(int index)
      {
        while (index >= bitsets.size()) {
          bitsets.add(null);
        }
        BitSet set = bitsets.get(index);
        if (set == null) {
          bitsets.set(index, set = new BitSet());
        }
        return set;
      }

      @Override
      public Builder buildDescriptor(ValueDesc desc, Builder builder) throws IOException
      {
        String fileName = String.format("met_%s.inverted", columnName);
        GenericIndexedWriter<ImmutableBitmap> writer = new GenericIndexedWriter<>(ioPeon, fileName, STRATEGY);
        writer.open();
        for (int i = 0; i < bitsets.size(); i++) {
          BitSet b = bitsets.get(i);
          writer.add(b != null && !b.isEmpty() ? RoaringBitmapFactory.convert(b) : null);
        }
        writer.close();
        return builder.addSerde(new SerDe(writer));
      }
    };
  }

  public static class SerDe implements ColumnPartSerde
  {
    private final GenericIndexedWriter<ImmutableBitmap> writer;

    @JsonCreator
    public SerDe()
    {
      writer = null;
    }

    public SerDe(GenericIndexedWriter<ImmutableBitmap> writer)
    {
      this.writer = Preconditions.checkNotNull(writer);
    }

    @Override
    public Serializer getSerializer()
    {
      return writer;
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory)
        {
          builder.setBitmapIndex(
              new BitmapIndexColumnPartSupplier(
                  RoaringBitmapSerdeFactory.bitmapFactory,
                  GenericIndexed.read(buffer, STRATEGY),
                  new ColumnPartProvider<Dictionary<String>>()
                  {
                    @Override
                    public int numRows()
                    {
                      return builder.getNumRows();    // set by previous ComplexColumnPartSerde, maybe
                    }

                    @Override
                    public long getSerializedSize()
                    {
                      return 0;   // not serialized, actually
                    }

                    @Override
                    public Dictionary<String> get()
                    {
                      return new ToStringDictionary();
                    }
                  }
              )
          );
        }
      };
    }
  }
}
