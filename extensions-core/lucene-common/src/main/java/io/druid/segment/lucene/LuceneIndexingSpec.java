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
import com.google.common.base.Preconditions;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.IOPeon;
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

/**
 */
@JsonTypeName("lucene")
public class LuceneIndexingSpec implements SecondaryIndexingSpec
{
  public static LuceneIndexingSpec of(String textAnalyzer, LuceneIndexingStrategy... strategies)
  {
    return new LuceneIndexingSpec(textAnalyzer, Arrays.asList(strategies), false);
  }

  private final String textAnalyzer;
  private final List<LuceneIndexingStrategy> strategies;
  private final boolean indexOnly;

  @JsonCreator
  public LuceneIndexingSpec(
      @JsonProperty("textAnalyzer") String textAnalyzer,
      @JsonProperty("strategies") List<LuceneIndexingStrategy> strategies,
      @JsonProperty("indexOnly") boolean indexOnly
  )
  {
    this.textAnalyzer = textAnalyzer;
    this.strategies = strategies == null ? ImmutableList.of() : strategies;
    this.indexOnly = indexOnly;
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
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)   // omit when false -> old spec JSON unchanged
  public boolean isIndexOnly()
  {
    return indexOnly;
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
    if (indexOnly != that.indexOnly) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(textAnalyzer, strategies, indexOnly);
  }

  @Override
  public MetricColumnSerializer serializer(String columnName, ValueDesc type, Iterable<Object> values)
  {
    if (GuavaUtils.isNullOrEmpty(strategies)) {
      return MetricColumnSerializer.DUMMY;
    }
    final List<LuceneIndexingStrategy> replaced = GuavaUtils.transform(
        strategies, s -> s.replaceFieldIfNull(columnName)
    );
    final List<LuceneFieldGenerator> generators = GuavaUtils.transform(
        replaced, strategy -> strategy.createIndexableField(type, values)
    );

    return new MetricColumnSerializer()
    {
      private IndexWriter writer;

      @Override
      public void open(IOPeon ioPeon)
      {
        writer = Lucenes.buildRamWriter(ioPeon.makeOutputFile(columnName + ".lucene"), textAnalyzer, replaced);
      }

      @Override
      public void serialize(int rowNum, Object obj) throws IOException
      {
        final Document doc = new Document();
        for (LuceneFieldGenerator generator : generators) {
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
        for (LuceneFieldGenerator generator : generators) {
          CloseQuietly.close(generator);
        }
      }

      @Override
      public ColumnDescriptor.Builder buildDescriptor(
          IOPeon ioPeon,
          ColumnDescriptor.Builder builder
      )
      {
        if (writer.getDocStats().numDocs > 0) {
          builder.addSerde(getSerde(writer))
                 .addDescriptor(descriptor(columnName));
        }
        return builder;
      }
    };
  }

  // Merge existing lucene indexes physically (addIndexes + forceMerge), instead of re-indexing from rows.
  // Mandatory for index-only columns (original text is gone). Returns null when the sources are not lucene
  // indexes (e.g. fresh ingestion) so the caller falls back to the row-based serializer(). Callers MUST pass
  // `sources` already ordered to match the merged row order (see IndexMergerV9's time-disjoint guard), so the
  // docID == row-ordinal identity survives.
  @Override
  public MetricColumnSerializer merger(String columnName, ValueDesc type, List<Column> sources)
  {
    final List<LuceneIndex> indexes = Lists.newArrayList();
    for (Column source : sources) {
      final ExternalIndexProvider<LuceneIndex> provider =
          source == null ? null : source.getExternalIndex(LuceneIndex.class);
      if (provider == null) {
        return null;   // not a lucene index -> let the caller build from rows
      }
      indexes.add(provider.get());
    }
    if (GuavaUtils.isNullOrEmpty(strategies)) {
      return MetricColumnSerializer.DUMMY;
    }
    return new MetricColumnSerializer()
    {
      private IndexWriter writer;

      @Override
      public void open(IOPeon ioPeon) throws IOException
      {
        final List<DirectoryReader> readers = GuavaUtils.transform(indexes, LuceneIndex::getReader);
        writer = Lucenes.mergeTo(ioPeon.makeOutputFile(columnName + ".lucene"), readers);
      }

      @Override
      public void serialize(int rowNum, Object obj)
      {
        // no-op: documents come from the source indexes, not the (absent) per-row values
      }

      @Override
      public void close() throws IOException
      {
        for (LuceneIndex index : indexes) {
          CloseQuietly.close(index);
        }
      }

      @Override
      public ColumnDescriptor.Builder buildDescriptor(IOPeon ioPeon, ColumnDescriptor.Builder builder)
      {
        // index-only: no base value column, only the merged secondary index (mirror ComplexColumnSerializer)
        builder.setValueType(type.isString() ? ValueDesc.STRING : type);
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
      strategy = strategy.replaceFieldIfNull(column);
      String desc = strategy.getFieldDescriptor();
      if (desc != null) {
        descriptors.put(strategy.getFieldName(), desc);
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
        public long getSerializedSize()
        {
          return Lucenes.sizeOf(luceneIndexer);
        }

        @Override
        public long writeToChannel(WritableByteChannel channel) throws IOException
        {
          return Lucenes.writeTo(luceneIndexer, channel);
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
          // Two ways to open the index. WHOLE: the column payload was fully materialized (heap/mmap/whole-fetch) — wrap
          // it in a buffer-backed reader. RANGE (range-serving): only the column HEAD (descriptor + file-offset table)
          // was fetched; the reader range-reads each index file on demand (term-dict block + a term's postings, tens
          // of KB) instead of the whole ~tens-of-MB index. Both feed the same secondary-index provider below.
          final java.util.function.Supplier<DirectoryReader> readerSupplier;
          final boolean closeReaderPerUse;   // WHOLE: each get() opens+closes its own; RANGE: one shared memoized reader
          final long length;
          final int numRows;
          final int fromBase = builder.getNumRows();   // set by the base value column (index-only columns: -1)
          if (builder.getRangeMapper() != null) {
            final io.druid.java.util.common.io.smoosh.SmooshedFileMapper m = builder.getRangeMapper();
            final String col = builder.getRangeColumn();
            length = buffer.getInt();   // payload length prefix; buffer (the head) is now at the index file table
            final Lucenes.RangeTable table = Lucenes.parseRangeTable(buffer);
            // Memoize the range reader per column (segment): opening it range-fetches the index structure (.tip term
            // index + headers/footers), the dominant per-segment cost at scale — so open ONCE and share across the
            // maxDoc probe, every get(), and every query (warm reuse). Its RangeFetchIndexInputs hold no OS handle, so
            // it needs no per-query close; it dies with the column when the segment is dropped/evicted.
            final DirectoryReader[] memo = new DirectoryReader[1];
            readerSupplier = () -> {
              if (memo[0] == null) {
                synchronized (memo) {
                  if (memo[0] == null) {
                    memo[0] = Lucenes.rangeReader(table, (o, l) -> m.fetchInColumn(col, o, l));
                  }
                }
              }
              return memo[0];
            };
            closeReaderPerUse = false;
            numRows = fromBase >= 0 ? fromBase : readerSupplier.get().maxDoc();   // reuses the shared reader
          } else {
            final ByteBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer);
            length = bufferToUse.remaining();
            readerSupplier = () -> Lucenes.readFrom(bufferToUse.asReadOnlyBuffer());
            closeReaderPerUse = true;
            // Normal columns: the base value column (read before this part) already set numRows — no extra IO.
            // Index-only columns have no base column, so derive numRows from the lucene index (one reader-open).
            numRows = fromBase >= 0 ? fromBase : Lucenes.maxDoc(bufferToUse.asReadOnlyBuffer());
          }

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
                  return new LuceneIndex()
                  {
                    final DirectoryReader reader = readerSupplier.get();

                    @Override
                    public void close() throws IOException
                    {
                      if (closeReaderPerUse) {   // range mode shares one memoized reader — don't close it per query
                        reader.close();
                      }
                    }

                    @Override
                    public BitmapHolder filterFor(Query query, FilterContext context, String attachment, int limit)
                    {
                      try {
                        final IndexSearcher searcher = createIndexSearcher(reader);
                        final long _t0 = System.nanoTime();
                        final ImmutableBitmap bitmap;
                        if (limit <= 0 && attachment == null) {
                          // pure filter/count: collect every match, NO scoring — skips the maxDoc-sized
                          // TopScoreDocCollector heap + norms that dominated per-segment CPU on a wide filter.
                          bitmap = Lucenes.collectAll(searcher, query, context);
                        } else {
                          // top `limit` by score (or scores requested via attachment): keep the scoring collector.
                          final int effective = limit > 0 ? Math.min(limit, numRows) : numRows;
                          bitmap = Lucenes.toBitmap(searcher.search(query, effective), context, attachment);
                        }
                        io.druid.java.util.common.RangeProf.searchNanos.addAndGet(System.nanoTime() - _t0);
                        io.druid.java.util.common.RangeProf.searchCount.incrementAndGet();
                        return BitmapHolder.exact(bitmap);
                      } catch (IOException e) {
                        throw Throwables.propagate(e);
                      }
                    }

                    @Override
                    public TopDocs query(Query query)
                    {
                      try {
                        return createIndexSearcher(reader).search(query, numRows);
                      } catch (IOException e) {
                        throw Throwables.propagate(e);
                      }
                    }

                    @Override
                    public IndexSearcher searcher()
                    {
                      return createIndexSearcher(reader);
                    }

                    @Override
                    public DirectoryReader getReader()
                    {
                      return reader;
                    }
                  };
                }
              }
          );
        }
      };
    }

    protected IndexSearcher createIndexSearcher(DirectoryReader reader)
    {
      return new IndexSearcher(reader);
    }
  }
}
