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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.LuceneIndexingStrategy;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LatLonPointPrototypeQueries;
import org.apache.lucene.search.TopDocs;

import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("lucene.nearest")
public class LuceneNearestFilter extends Lucenes.LuceneSelector
{
  private final double latitude;
  private final double longitude;
  private final int count;

  @JsonCreator
  public LuceneNearestFilter(
      @JsonProperty("field") String field,
      @JsonProperty("latitude") double latitude,
      @JsonProperty("longitude") double longitude,
      @JsonProperty("count") int count,
      @JsonProperty("scoreField") String scoreField
  )
  {
    super(field, scoreField);
    this.latitude = latitude;
    this.longitude = longitude;
    this.count = count;
    Preconditions.checkArgument(count > 0, "count should be > 0");
  }

  @JsonProperty
  public double getLatitude()
  {
    return latitude;
  }

  @JsonProperty
  public double getLongitude()
  {
    return longitude;
  }

  @JsonProperty
  public int getCount()
  {
    return count;
  }

  @Override
  protected Object[] params()
  {
    return new Object[]{field, latitude, longitude, count, scoreField};
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LUCENE_NEAREST_CACHE_ID)
                  .append(field)
                  .append(latitude)
                  .append(longitude)
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
    return new LuceneNearestFilter(replaced, latitude, longitude, count, scoreField);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        Column column = Preconditions.checkNotNull(
            Lucenes.findLuceneColumn(field, context.internal()), "no lucene index for [%s]", field
        );
        String luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, LuceneIndexingStrategy.TEXT_DESC),
            "cannot find lucene field name in [%s:%s]", column.getName(), column.getColumnDescs().keySet()
        );
        LuceneIndex index = column.getSecondaryIndex();
        try {
          IndexSearcher searcher = index.searcher();
          TopDocs searched = LatLonPointPrototypeQueries.nearest(searcher, luceneField, latitude, longitude, count);
          return BitmapHolder.exact(Lucenes.toBitmap(searched, context, scoreField));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        throw new UnsupportedOperationException("value matcher");
      }

      @Override
      public String toString()
      {
        return LuceneNearestFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "LuceneNearestFilter{" +
           "field='" + field + '\'' +
           ", latitude=" + latitude +
           ", longitude=" + longitude +
           ", count=" + count +
           (scoreField == null ? "" : ", scoreField='" + scoreField + '\'') +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, latitude, longitude, count, scoreField);
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

    LuceneNearestFilter that = (LuceneNearestFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (latitude != that.latitude) {
      return false;
    }
    if (longitude != that.longitude) {
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
