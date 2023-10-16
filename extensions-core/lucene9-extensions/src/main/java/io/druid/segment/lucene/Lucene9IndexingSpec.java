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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.UOE;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.serde.ColumnPartSerde;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 *
 */
@JsonTypeName("lucene9")
public class Lucene9IndexingSpec extends LuceneIndexingSpec
{
  public static LuceneIndexingSpec of(String textAnalyzer, LuceneIndexingStrategy... strategies)
  {
    return new Lucene9IndexingSpec(textAnalyzer, Arrays.asList(strategies));
  }

  @JsonCreator
  public Lucene9IndexingSpec(
      @JsonProperty("textAnalyzer") String textAnalyzer,
      @JsonProperty("strategies") List<LuceneIndexingStrategy> strategies
  )
  {
    super(textAnalyzer, strategies);
  }

  @Override
  protected ColumnPartSerde getSerde(IndexWriter writer)
  {
    return new SerDe(writer);
  }

  @JsonTypeName("lucene9")
  public static class SerDe extends LuceneIndexingSpec.SerDe
  {
    @JsonCreator
    public SerDe() {}

    public SerDe(IndexWriter writer)
    {
      super(writer);
    }

    @Override
    protected IndexSearcher createIndexSearcher(DirectoryReader reader)
    {
      return new IndexSearcher(reader)
      {
        @Override
        public Query rewrite(Query original) throws IOException
        {
          return BitmapRelay.isRelay(original) ? original : super.rewrite(original);
        }

        @Override
        public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException
        {
          ImmutableBitmap bitmap = BitmapRelay.unwrap(query);
          if (bitmap == null) {
            return super.createWeight(query, scoreMode, boost);
          }
          return new Weight(query)
          {
            @Override
            public boolean isCacheable(LeafReaderContext ctx)
            {
              return false;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException
            {
              return Explanation.noMatch("From external bitmap");
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException
            {
              return new Scorer(this)
              {
                private final BitSet bitset = RoaringBitmapFactory.toBitset(bitmap);

                @Override
                public DocIdSetIterator iterator()
                {
                  return new BitSetIterator(new FixedBitSet(bitset.toLongArray(), bitset.length()), 0);
                }

                @Override
                public float getMaxScore(int upTo) throws IOException
                {
                  throw new UOE("getMaxScore");
                }

                @Override
                public float score() throws IOException
                {
                  throw new UOE("score");
                }

                @Override
                public int docID()
                {
                  throw new UOE("docID");
                }
              };
            }
          };
        }
      };
    }
  }
}
