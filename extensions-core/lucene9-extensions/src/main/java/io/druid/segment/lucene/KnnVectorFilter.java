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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilterCacheKey;
import io.druid.query.filter.Filter;
import io.druid.segment.column.Column;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("lucene.knn.vector")
public class KnnVectorFilter extends LuceneSelector
{
  private final float[] vector;
  private final int count;

  @JsonCreator
  public KnnVectorFilter(
      @JsonProperty("field") String field,
      @JsonProperty("vector") float[] vector,
      @JsonProperty("count") int count,
      @JsonProperty("scoreField") String scoreField
  )
  {
    super(field, scoreField);
    this.vector = Preconditions.checkNotNull(vector, "'vector' should not be null");
    this.count = count;
    Preconditions.checkArgument(count > 0, "count should be > 0");
  }

  @JsonProperty
  public float[] getVector()
  {
    return vector;
  }

  @JsonProperty
  public int getCount()
  {
    return count;
  }

  @Override
  protected Object[] params()
  {
    return new Object[]{field, vector, count, scoreField};
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LUCENE_KNN_CACHE_ID)
                  .append(field)
                  .append(vector)
                  .append(count)
                  .append(scoreField);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new KnnVectorFilter(replaced, vector, count, scoreField);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter.BitmapOnly()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        Column column = Preconditions.checkNotNull(
            Lucenes.findColumnWithLuceneIndex(field, context.internal()), "no lucene index on [%s]", field
        );
        String luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, TextIndexingStrategy.TYPE_NAME, JsonIndexingStrategy.TYPE_NAME),
            "cannot find lucene field name in [%s:%s]", column.getName(), column.getColumnDescs().keySet()
        );
        Query filter = BitmapRelay.queryFor(context.baseBitmap());
        KnnFloatVectorQuery query = new KnnFloatVectorQuery(luceneField, vector, count, filter);
        LuceneIndex lucene = column.getExternalIndex(LuceneIndex.class).get();
        try {
          TopDocs searched = lucene.searcher().search(query, count);
          return BitmapHolder.exact(Lucenes.toBitmap(searched, context, scoreField));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString()
      {
        return KnnVectorFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "KnnVectorFilter{" +
           "field='" + field + '\'' +
           ", vector=" + Arrays.toString(vector) +
           ", count=" + count +
           (scoreField == null ? "" : ", scoreField='" + scoreField + '\'') +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, Arrays.hashCode(vector), count, scoreField);
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

    KnnVectorFilter that = (KnnVectorFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!Arrays.equals(vector, that.vector)) {
      return false;
    }
    if (count != that.count) {
      return false;
    }
    if (!Objects.equals(scoreField, that.scoreField)) {
      return false;
    }
    return true;
  }
}
