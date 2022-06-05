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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.DictionaryPartBuilder;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.FSTHolder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.SimpleRelaySerializer;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.LuceneIndexInput;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class LuceneFSTSerDe extends FSTBuilder implements ColumnPartSerde, DictionaryPartBuilder
{
  private ColumnPartSerde.Serializer serializer;

  @JsonCreator
  public LuceneFSTSerDe(Float reduction)
  {
    super(reduction);
  }

  @Override
  public ColumnPartSerde.Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public ColumnPartSerde done(int cardinality) throws IOException
  {
    FST<Long> fst = make(cardinality);
    if (fst != null) {
      this.serializer = new SimpleRelaySerializer(save(fst));
      return this;
    }
    return null;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory factory) throws IOException
      {
        builder.setFST(new ColumnPartProvider.ExternalPart<FSTHolder>()
        {
          private final ByteBuffer block = ByteBufferSerializer.prepareForRead(buffer);

          @Override
          public String source()
          {
            return LuceneFSTSerDe.this.getClass().getAnnotation(JsonTypeName.class).value();
          }

          @Override
          public Class classOfObject()
          {
            return FST.class;
          }

          @Override
          public int numRows()
          {
            return builder.getNumRows();
          }

          @Override
          public long getSerializedSize()
          {
            return block.remaining();
          }

          @Override
          public FSTHolder get()
          {
            final DataInput input = LuceneIndexInput.newInstance("FST", block.slice(), block.remaining());  // slice !
            try {
              return new FSTHolder()
              {
                private final FST<Long> fst = load(input);

                @Override
                @SuppressWarnings("unchecked")
                public <T> T unwrap(Class<T> clazz)
                {
                  return clazz == FST.class ? (T) fst : null;
                }
              };
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        });
      }
    };
  }

  protected abstract byte[] save(FST<Long> make) throws IOException;

  protected abstract FST<Long> load(DataInput input) throws IOException;
}
