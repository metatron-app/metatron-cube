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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.AttachmentVirtualColumn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.LuceneIndexingStrategy;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.query")
public class LuceneQueryFilter extends DimFilter.LuceneFilter implements DimFilter.VCInflator
{
  public static LuceneQueryFilter of(String field, String expression, String scoreField)
  {
    return new LuceneQueryFilter(field, null, expression, scoreField, false);
  }

  private final String field;
  private final String scoreField;
  private final String analyzer;
  private final String expression;
  private final boolean inflated;   // marker not to add virtual column again. todo: find better way

  @JsonCreator
  public LuceneQueryFilter(
      @JsonProperty("field") String field,
      @JsonProperty("analyzer") String analyzer,
      @JsonProperty("expression") String expression,
      @JsonProperty("scoreField") String scoreField,
      @JsonProperty("inflated") boolean inflated
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.analyzer = Objects.toString(analyzer, "standard");
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
    this.scoreField = scoreField;
    this.inflated = inflated;
  }

  @Override
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getScoreField()
  {
    return scoreField;
  }

  @JsonProperty
  public boolean isInflated()
  {
    return inflated;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_QUERY_CACHE_ID)
                  .append(field).sp()
                  .append(analyzer).sp()
                  .append(expression).sp()
                  .append(scoreField);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneQueryFilter(replaced, analyzer, expression, scoreField, inflated);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public VirtualColumn inflate()
  {
    return !inflated && scoreField != null ? new AttachmentVirtualColumn(scoreField, ValueDesc.FLOAT) : null;
  }

  @Override
  public DimFilter rewrite(QuerySegmentWalker walker, io.druid.query.Query parent)
  {
    return inflated || scoreField == null ? this : new LuceneQueryFilter(field, analyzer, expression, scoreField, true);
  }

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    return this;    // has nothing to do
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getBitmapIndex(FilterContext context)
      {
        Column column = Preconditions.checkNotNull(
            Lucenes.findLuceneColumn(field, context.indexSelector()), "no lucene index on [%s]", field
        );
        String luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, LuceneIndexingStrategy.TEXT_DESC),
            "cannot find lucene field name in [%s]", column.getName()
        );
        LuceneIndex lucene = column.getLuceneIndex();
        try {
          QueryParser parser = new QueryParser(luceneField, Lucenes.createAnalyzer(analyzer));
          Query query = parser.parse(expression);
          return lucene.filterFor(query, context, scoreField);
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
           ", scoreField='" + scoreField + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, analyzer, expression, scoreField);
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
    if (!Objects.equals(scoreField, that.scoreField)) {
      return false;
    }

    return true;
  }
}
