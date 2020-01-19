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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.LuceneIndexingStrategy;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.query")
public class LuceneQueryFilter extends DimFilter.LuceneFilter
{
  private final String field;
  private final String analyzer;
  private final String expression;

  @JsonCreator
  public LuceneQueryFilter(
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_QUERY_CACHE_ID)
                  .append(field).sp()
                  .append(analyzer).sp()
                  .append(expression);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneQueryFilter(replaced, analyzer, expression);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
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
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
      {
        Column column = Preconditions.checkNotNull(
            Lucenes.findLuceneColumn(field, selector), "no lucene index for [%s]", field
        );
        String luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, LuceneIndexingStrategy.TEXT_DESC),
            "cannot find lucene field name in [%s]", column.getName()
        );
        LuceneIndex lucene = column.getLuceneIndex();
        try {
          QueryParser parser = new QueryParser(luceneField, Lucenes.createAnalyzer(analyzer));
          Query query = parser.parse(expression);
          return lucene.filterFor(query, null);
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
        return LuceneQueryFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "LuceneQueryFilter{" +
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

    LuceneQueryFilter that = (LuceneQueryFilter) o;

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
