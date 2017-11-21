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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.Lucenes;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class LuceneFilter implements DimFilter
{
  private final String field;
  private final String analyzer;
  private final String expression;

  @JsonCreator
  public LuceneFilter(
      @JsonProperty("field") String field,
      @JsonProperty("analyzer") String analyzer,
      @JsonProperty("expression") String expression
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.analyzer = Objects.toString(analyzer, "standard");
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public String getAnalyzer()
  {
    return analyzer;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldBytes = StringUtils.toUtf8(field);
    byte[] analyzerBytes = StringUtils.toUtf8WithNullToEmpty(analyzer);
    byte[] expressionBytes = StringUtils.toUtf8(expression);
    return ByteBuffer.allocate(3 + fieldBytes.length + analyzerBytes.length + expressionBytes.length)
                     .put(DimFilterCacheHelper.LUCENE_CACHE_ID)
                     .put(fieldBytes).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(analyzerBytes).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        // todo values are not included in lucene index itself.. I think we can embed values in LuceneIndex class
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, EnumSet<BitmapType> using)
      {
        // lucene field-name == druid column-name
        LuceneIndex index = Preconditions.checkNotNull(selector.getLuceneIndex(field), "no lucene index for " + field);
        try {
          QueryParser parser = new QueryParser(field, Lucenes.createAnalyzer(analyzer));
          Query query = parser.parse(expression);

          return index.filterFor(query);
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
        return LuceneFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "LuceneFilter{" +
           "field='" + field + '\'' +
           ", analyzer='" + analyzer + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, analyzer, expression);
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

    LuceneFilter that = (LuceneFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!analyzer.equals(that.analyzer)) {
      return false;
    }
    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }
}
