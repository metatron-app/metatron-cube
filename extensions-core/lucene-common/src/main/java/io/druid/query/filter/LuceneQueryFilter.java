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
import com.google.common.collect.ImmutableMap;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.JsonIndexingStrategy;
import io.druid.segment.lucene.LuceneSelector;
import io.druid.segment.lucene.Lucenes;
import io.druid.segment.lucene.TextIndexingStrategy;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.Query;

import java.util.Map;
import java.util.Objects;

/**
 *
 */
@JsonTypeName("lucene.query")
public class LuceneQueryFilter extends LuceneSelector implements DimFilter.VCInflator
{
  public static LuceneQueryFilter of(String field, String expression, String scoreField)
  {
    return new LuceneQueryFilter(field, null, expression, null, scoreField);
  }

  private final String analyzer;
  private final String expression;
  private final Map<String, String> types;

  @JsonCreator
  public LuceneQueryFilter(
      @JsonProperty("field") String field,
      @JsonProperty("analyzer") String analyzer,
      @JsonProperty("expression") String expression,
      @JsonProperty("types") Map<String, String> types,
      @JsonProperty("scoreField") String scoreField
  )
  {
    super(field, scoreField);
    this.analyzer = Objects.toString(analyzer, "standard");
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
    this.types = types == null ? ImmutableMap.of() : types;
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
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getTypes()
  {
    return types;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LUCENE_QUERY_CACHE_ID)
                  .append(field).sp()
                  .append(analyzer).sp()
                  .append(expression).sp()
                  .append(types).sp()
                  .append(scoreField);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneQueryFilter(replaced, analyzer, expression, types, scoreField);
  }

  @Override
  protected Object[] params()
  {
    return new Object[]{field, analyzer, expression, types, scoreField};
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
            Lucenes.findColumnWithLuceneIndex(field, context.internal()), "no lucene index on [%s]", field
        );
        String luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, TextIndexingStrategy.TYPE_NAME, JsonIndexingStrategy.TYPE_NAME),
            "cannot find lucene field name in [%s:%s]", column.getName(), column.getColumnDescs().keySet()
        );
        StandardQueryParser parser = new StandardQueryParser(Lucenes.createAnalyzer(analyzer));
        Map<String, PointsConfig> configMap = Lucenes.asPointConfig(types);
        if (!configMap.isEmpty()) {
          parser.setPointsConfigMap(configMap);
        }

        LuceneIndex lucene = column.getExternalIndex(LuceneIndex.class).get();
        try {
          Query query = parser.parse(expression, luceneField);
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
           (types.isEmpty() ? "" : ", types=" + types) +
           (scoreField == null ? "" : ", scoreField='" + scoreField + '\'') +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, analyzer, expression, types, scoreField);
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
    if (!Objects.equals(types, that.types)) {
      return false;
    }
    if (!Objects.equals(scoreField, that.scoreField)) {
      return false;
    }

    return true;
  }
}
