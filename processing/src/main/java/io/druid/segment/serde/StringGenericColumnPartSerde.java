/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.ValueType;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.Lucenes;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.GenericIndexed;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class StringGenericColumnPartSerde implements ColumnPartSerde
{
  private final Serializer serializer;

  @JsonCreator
  public StringGenericColumnPartSerde()
  {
    this(null);
  }

  public StringGenericColumnPartSerde(final ComplexColumnSerializer delegate)
  {
    this.serializer = delegate;
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
      {
        final BitmapFactory factory = serdeFactory.getBitmapFactory();
        final GenericIndexed<String> indexed = GenericIndexed.read(buffer, GenericIndexed.STRING_STRATEGY);
        builder.setType(ValueType.STRING)
               .setHasMultipleValues(false)
               .setGenericColumn(new StringColumnPartSupplier(indexed));

        if (buffer.remaining() > 0) {
          final ByteBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer);
          final int length = bufferToUse.remaining();

          builder.setLuceneIndex(
              new ColumnPartProvider<LuceneIndex>()
              {
                @Override
                public int size()
                {
                  return indexed.size();
                }

                @Override
                public long getSerializedSize()
                {
                  return length;
                }

                @Override
                public LuceneIndex get()
                {
                  final DirectoryReader reader = Lucenes.deserializeWithRuntimeException(bufferToUse.asReadOnlyBuffer());

                  return new LuceneIndex()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      reader.close();
                    }

                    @Override
                    public ValueType type()
                    {
                      return ValueType.STRING;
                    }

                    @Override
                    public ImmutableBitmap filterFor(Query query)
                    {
                      final IndexSearcher searcher = new IndexSearcher(reader);
                      try {
                        TopDocs searched = searcher.search(query, indexed.size());
                        if (searched.totalHits == 0) {
                          return factory.makeEmptyImmutableBitmap();
                        }
                        MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
                        for (ScoreDoc scoreDoc : searched.scoreDocs) {
                          bitmap.add(scoreDoc.doc);
                        }
                        return factory.makeImmutableBitmap(bitmap);
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
                    }

                    @Override
                    public int rows()
                    {
                      return indexed.size();
                    }
                  };
                }
              }
          );
        }
      }
    };
  }
}
