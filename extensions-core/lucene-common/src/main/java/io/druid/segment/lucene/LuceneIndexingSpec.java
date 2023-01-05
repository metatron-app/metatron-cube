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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.serde.ColumnPartSerde;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 */
@JsonTypeName("lucene")
public class LuceneIndexingSpec implements SecondaryIndexingSpec.WithDescriptor
{
  public static LuceneIndexingSpec of(String textAnalyzer, LuceneIndexingStrategy... strategies)
  {
    return new LuceneIndexingSpec(textAnalyzer, Arrays.asList(strategies));
  }

  private final String textAnalyzer;
  private final List<LuceneIndexingStrategy> strategies;

  @JsonCreator
  public LuceneIndexingSpec(
      @JsonProperty("textAnalyzer") String textAnalyzer,
      @JsonProperty("strategies") List<LuceneIndexingStrategy> strategies
  )
  {
    this.textAnalyzer = textAnalyzer;
    this.strategies = strategies == null ? ImmutableList.<LuceneIndexingStrategy>of() : strategies;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTextAnalyzer()
  {
    return textAnalyzer;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<LuceneIndexingStrategy> getStrategies()
  {
    return strategies;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LuceneIndexingSpec that = (LuceneIndexingSpec) o;

    if (!Objects.equals(textAnalyzer, that.textAnalyzer)) {
      return false;
    }
    if (!Objects.equals(strategies, that.strategies)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(textAnalyzer, strategies);
  }

  @Override
  public MetricColumnSerializer serializer(String columnName, final ValueDesc type)
  {
    if (GuavaUtils.isNullOrEmpty(strategies)) {
      return MetricColumnSerializer.DUMMY;
    }
    final Iterable<LuceneIndexingStrategy> replaced = Iterables.transform(
        strategies, s -> s.getFieldName() == null ? s.withFieldName(columnName) : s
    );
    final List<Function<Object, Field[]>> generators = GuavaUtils.transform(
        replaced, strategy -> strategy.createIndexableField(type)
    );
    final IndexWriter writer = Lucenes.buildRamWriter(textAnalyzer);

    return new MetricColumnSerializer()
    {
      @Override
      public void serialize(int rowNum, Object obj) throws IOException
      {
        final Document doc = new Document();
        for (Function<Object, Field[]> generator : generators) {
          Field[] fields = generator.apply(obj);
          if (fields != null) {
            for (Field field : fields) {
              doc.add(field);
            }
          }
        }
        writer.addDocument(doc);
      }

      @Override
      public void close() throws IOException
      {
        writer.commit();
      }

      @Override
      public ColumnDescriptor.Builder buildDescriptor(ValueDesc desc, ColumnDescriptor.Builder builder)
      {
        if (writer.getDocStats().numDocs > 0) {
          builder.addSerde(getSerde(writer))
                 .addDescriptor(descriptor(columnName));
        }
        return builder;
      }
    };
  }

  protected ColumnPartSerde getSerde(IndexWriter writer)
  {
    return new SerDe(writer);
  }

  @Override
  public Map<String, String> descriptor(String column)
  {
    Map<String, String> descriptors = Maps.newLinkedHashMap();
    for (LuceneIndexingStrategy strategy : strategies) {
      String desc = strategy.getFieldDescriptor();
      if (desc != null) {
        descriptors.put(Optional.ofNullable(strategy.getFieldName()).orElse(column), desc);
      }
    }
    return descriptors;
  }

  @JsonTypeName("lucene")
  public static class SerDe implements ColumnPartSerde
  {
    private final IndexWriter luceneIndexer;

    @JsonCreator
    public SerDe()
    {
      luceneIndexer = null;
    }

    public SerDe(IndexWriter luceneIndexer)
    {
      this.luceneIndexer = Preconditions.checkNotNull(luceneIndexer);
    }

    @Override
    public Serializer getSerializer()
    {
      return new Serializer()
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

          builder.addSecondaryIndex(
              new ExternalIndexProvider<LuceneIndex>()
              {
                @Override
                public String source()
                {
                  return SerDe.this.getClass().getAnnotation(JsonTypeName.class).value();
                }

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
                public Class<? extends LuceneIndex> provides()
                {
                  return LuceneIndex.class;
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
                    public BitmapHolder filterFor(Query query, FilterContext context, String attachment)
                    {
                      return BitmapHolder.exact(Lucenes.toBitmap(query(query), context, attachment));   // really?
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
                  };
                }
              }
          );
        }
      };
    }
  }
}
