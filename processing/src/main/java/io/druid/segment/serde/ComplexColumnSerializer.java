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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.lucene.LuceneIndexingSpec;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.Lucenes;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

public class ComplexColumnSerializer implements GenericColumnSerializer, ColumnPartSerde.Serializer
{
  public static ComplexColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      ComplexMetricSerde serde,
      LuceneIndexingSpec indexingSpec
  )
  {
    return new ComplexColumnSerializer(ioPeon, filenameBase, serde, indexingSpec);
  }

  private final IOPeon ioPeon;
  private final String columnName;
  private final ComplexMetricSerde serde;

  private String minValue;
  private String maxValue;
  private int numNulls;

  private final List<Function<Object, Field>> fieldGenerators;
  private final IndexWriter luceneIndexer;

  private GenericIndexedWriter writer;

  public ComplexColumnSerializer(
      IOPeon ioPeon,
      String columnName,
      ComplexMetricSerde serde,
      LuceneIndexingSpec indexingSpec
  )
  {
    this.ioPeon = ioPeon;
    this.columnName = columnName;
    this.serde = serde;
    if (indexingSpec != null) {
      fieldGenerators = Lists.newArrayList();
      ValueDesc type = ValueDesc.of(serde.getTypeName());
      if (type.isString()) {
        Preconditions.checkArgument(
            GuavaUtils.isNullOrEmpty(indexingSpec.getStrategies()),
            "string column cannot have indexing strategy"
        );
        fieldGenerators.add(Lucenes.makeTextFieldGenerator(columnName));
      } else {
        fieldGenerators.addAll(Lists.transform(indexingSpec.getStrategies(), Lucenes.makeGenerator(type)));
      }
      luceneIndexer = Lucenes.buildRamWriter(indexingSpec.getTextAnalyzer());
    } else {
      fieldGenerators = null;
      luceneIndexer = null;
    }
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(
        ioPeon, String.format("%s.complex_column", columnName), serde.getObjectStrategy()
    );
    writer.open();
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(Object obj) throws IOException
  {
    writer.write(obj);
    if (luceneIndexer != null) {
      Document doc = new Document();
      for (Function<Object, Field> generator : fieldGenerators) {
        doc.add(generator.apply(obj));
      }
      luceneIndexer.addDocument(doc);
    }
    if (obj == null) {
      numNulls++;
    } else if (serde == StringMetricSerde.INSTANCE) {
      String value = (String) obj;
      minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
      maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
    }
  }

  @Override
  public Builder buildDescriptor(ValueDesc desc, Builder builder) throws IOException
  {
    if (ValueDesc.isString(desc)) {
      builder.setValueType(ValueType.STRING);
      builder.addSerde(new StringGenericColumnPartSerde(this));
    } else {
      builder.setValueType(ValueType.COMPLEX);
      builder.addSerde(
          ComplexColumnPartSerde.serializerBuilder()
                                .withTypeName(desc.typeName())
                                .withDelegate(this)
                                .build()
      );
    }
    if (luceneIndexer != null && luceneIndexer.numDocs() > 0) {
      builder.addSerde(new LuceneIndexPartSerDe(luceneIndexer));
    }
    return builder;
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    if (luceneIndexer != null) {
      luceneIndexer.commit();
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (numNulls > 0 && (minValue == null || maxValue == null)) {
      return null;
    }
    if (minValue == null || maxValue == null) {
      return ImmutableMap.<String, Object>of(
          "numNulls", numNulls
      );
    }
    return ImmutableMap.<String, Object>of(
        "min", minValue,
        "max", maxValue,
        "numNulls", numNulls
    );
  }

  public static class LuceneIndexPartSerDe implements ColumnPartSerde
  {
    private final IndexWriter luceneIndexer;
    private byte[] lucenePayload;

    @JsonCreator
    public LuceneIndexPartSerDe()
    {
      luceneIndexer = null;
    }

    public LuceneIndexPartSerDe(IndexWriter luceneIndexer)
    {
      this.luceneIndexer = Preconditions.checkNotNull(luceneIndexer);
    }

    @Override
    public Serializer getSerializer()
    {
      return new Serializer.Abstract()
      {
        @Override
        public long getSerializedSize() throws IOException
        {
          lucenePayload = Lucenes.serializeAndClose(luceneIndexer);
          return Ints.BYTES + lucenePayload.length;
        }

        @Override
        public void writeToChannel(WritableByteChannel channel) throws IOException
        {
          channel.write(ByteBuffer.wrap(Ints.toByteArray(lucenePayload.length)));
          channel.write(ByteBuffer.wrap(lucenePayload));
        }
      };
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(
            ByteBuffer buffer,
            ColumnBuilder builder,
            BitmapSerdeFactory serdeFactory
        ) throws IOException
        {
          final ByteBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer);
          final int length = bufferToUse.remaining();

          final int numRows = builder.getNumRows();
          final BitmapFactory factory = serdeFactory.getBitmapFactory();

          builder.setLuceneIndex(
              new ColumnPartProvider<LuceneIndex>()
              {
                @Override
                public int size()
                {
                  return numRows;
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
                        TopDocs searched = searcher.search(query, numRows);
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
                      return numRows;
                    }
                  };
                }
              }
          );
        }
      };
    }
  }
}
