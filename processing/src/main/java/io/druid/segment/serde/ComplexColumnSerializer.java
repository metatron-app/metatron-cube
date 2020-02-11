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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.CompressedComplexColumnSerializer;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.lucene.LuceneIndexingSpec;
import io.druid.segment.lucene.LuceneIndexingStrategy;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

public class ComplexColumnSerializer implements GenericColumnSerializer
{
  public static ComplexColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      ComplexMetricSerde serde,
      LuceneIndexingSpec indexingSpec,
      CompressionStrategy compression
  )
  {
    return new ComplexColumnSerializer(ioPeon, filenameBase, serde, indexingSpec, compression);
  }

  private final IOPeon ioPeon;
  private final String columnName;
  private final ComplexMetricSerde serde;
  private final CompressionStrategy compression;

  private String minValue;
  private String maxValue;
  private int numNulls;

  private final List<Function<Object, Field[]>> fieldGenerators;
  private final Map<String, String> descriptors;
  private final IndexWriter luceneIndexer;

  private ColumnPartWriter writer;

  public ComplexColumnSerializer(
      IOPeon ioPeon,
      String columnName,
      ComplexMetricSerde serde,
      LuceneIndexingSpec luceneSpec,
      CompressionStrategy compression
  )
  {
    this.ioPeon = ioPeon;
    this.columnName = columnName;
    this.serde = serde;
    this.compression = compression;

    ValueDesc type = ValueDesc.of(serde.getTypeName());
    List<LuceneIndexingStrategy> strategies = luceneSpec == null ? null : luceneSpec.getStrategies(columnName);
    if (!GuavaUtils.isNullOrEmpty(strategies)) {
      fieldGenerators = Lists.newArrayList(Lists.transform(strategies, Lucenes.makeGenerator(type)));
      luceneIndexer = Lucenes.buildRamWriter(luceneSpec.getTextAnalyzer());
    } else {
      fieldGenerators = null;
      luceneIndexer = null;
    }
    descriptors = LuceneIndexingSpec.getFieldDescriptors(strategies);
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    final String filenameBase = String.format("%s.complex_column", columnName);
    if (compression == null || compression == CompressionStrategy.UNCOMPRESSED) {
      writer = new GenericIndexedWriter(ioPeon, filenameBase, serde.getObjectStrategy());
    } else {
      writer = CompressedComplexColumnSerializer.create(ioPeon, filenameBase, compression, serde.getObjectStrategy());
    }
    writer.open();
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    writer.add(obj);
    if (luceneIndexer != null) {
      final Document doc = new Document();
      for (Function<Object, Field[]> generator : fieldGenerators) {
        Field[] fields = generator.apply(obj);
        if (fields != null) {
          for (Field field : fields) {
            doc.add(field);
          }
        }
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
  public Builder buildDescriptor(ValueDesc desc, Builder builder)
  {
    if (ValueDesc.isString(desc)) {
      builder.setValueType(ValueDesc.STRING);
      builder.addSerde(new StringColumnPartSerde(this));
    } else {
      builder.setValueType(ValueDesc.of(desc.typeName()));
      builder.addSerde(new ComplexColumnPartSerde(desc.typeName(), this));
    }
    if (luceneIndexer != null && luceneIndexer.getDocStats().numDocs > 0) {
      builder.addSerde(new LuceneIndexPartSerDe(luceneIndexer));
      builder.addDescriptor(descriptors);
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
          return Lucenes.sizeOf(luceneIndexer);
        }

        @Override
        public void writeToChannel(WritableByteChannel channel) throws IOException
        {
          Lucenes.writeTo(luceneIndexer, channel);
        }
      };
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory)
        {
          final ByteBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer);
          final int length = bufferToUse.remaining();

          final int numRows = builder.getNumRows();
          final BitmapFactory factory = serdeFactory.getBitmapFactory();

          final ValueDesc type  = builder.getType();

          builder.setLuceneIndex(
              new ColumnPartProvider<LuceneIndex>()
              {
                @Override
                public int numRows()
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
                  final DirectoryReader reader = Lucenes.readFrom(bufferToUse.asReadOnlyBuffer());
                  final IndexSearcher searcher = new IndexSearcher(reader);

                  return new LuceneIndex()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      reader.close();
                    }

                    @Override
                    public ValueDesc type()
                    {
                      return type;
                    }

                    @Override
                    public ImmutableBitmap filterFor(Query query, ImmutableBitmap baseBitmap)
                    {
                      return Lucenes.toBitmap(factory, query(query));
                    }

                    @Override
                    public boolean isExact()
                    {
                      return true;    // really?
                    }

                    @Override
                    public TopDocs query(Query query)
                    {
                      try {
                        return searcher.search(query, numRows);
                      }
                      catch (IOException e) {
                        throw Throwables.propagate(e);
                      }
                    }

                    @Override
                    public IndexSearcher searcher()
                    {
                      return searcher;
                    }

                    @Override
                    public int numRows()
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
