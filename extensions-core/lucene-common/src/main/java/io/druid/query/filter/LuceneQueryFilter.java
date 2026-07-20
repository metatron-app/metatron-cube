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
import io.druid.java.util.common.Throwables;
import com.google.common.collect.ImmutableMap;
import io.druid.common.KeyBuilder;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

import java.util.Map;
import java.util.Objects;

/**
 *
 */
@JsonTypeName("lucene.query")
public class LuceneQueryFilter extends LuceneSelector implements DimFilter.VCInflator
{
  // A bare fuzzy term (term~) over a huge free-text term dictionary (e.g. the credential-dump `raw` field) walks a
  // Levenshtein automaton across a large fraction of the .tim term dictionary — measured ~672x the term-dict reads of
  // a plain term (133,857 vs 199 .tim GETs/100-segs), enough to time a wide scan out. Requiring the first N chars to
  // match exactly bounds the automaton's entry into the term dict, pruning that scan by orders of magnitude. Override
  // with -Ddruid.lucene.fuzzyPrefixLength (0 restores stock Lucene behavior).
  private static final int FUZZY_PREFIX_LENGTH = Integer.getInteger("druid.lucene.fuzzyPrefixLength", 2);

  public static LuceneQueryFilter of(String field, String expression, String scoreField)
  {
    return new LuceneQueryFilter(field, null, expression, null, scoreField, 0, false, null);
  }

  private final String analyzer;
  private final String expression;
  private final Map<String, String> types;
  private final int limit;   // cap matches to the top `limit` docs per segment (0 = unlimited)
  // treat `expression` as a literal keyword (analyze + phrase) instead of query-parser syntax, so
  // keyword chars like @ : . _ never trip the parser or collapse into a match-all/OR query.
  private final boolean literal;
  // relevance floor: only docs scoring >= minScore enter the bitmap (NaN = no floor). Set by the two-pass /resolve
  // score path (pass 1 finds the global top-N cutoff, pass 2 replays with it so only ~N docs materialize).
  private final float minScore;

  @JsonCreator
  public LuceneQueryFilter(
      @JsonProperty("field") String field,
      @JsonProperty("analyzer") String analyzer,
      @JsonProperty("expression") String expression,
      @JsonProperty("types") Map<String, String> types,
      @JsonProperty("scoreField") String scoreField,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("literal") Boolean literal,
      @JsonProperty("minScore") Float minScore
  )
  {
    super(field, scoreField);
    this.analyzer = Objects.toString(analyzer, "standard");
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
    this.types = types == null ? ImmutableMap.of() : types;
    this.limit = limit == null ? 0 : limit;
    this.literal = literal != null && literal;
    this.minScore = minScore == null ? Float.NaN : minScore;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isLiteral()
  {
    return literal;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Float getMinScore()
  {
    return Float.isNaN(minScore) ? null : minScore;   // omit from JSON when unset -> old spec unchanged
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LUCENE_QUERY_CACHE_ID)
                  .append(field).sp()
                  .append(analyzer).sp()
                  .append(expression).sp()
                  .append(types).sp()
                  .append(scoreField).sp()
                  .append(limit).sp()
                  .append(literal).sp()
                  .append(Float.floatToIntBits(minScore));
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneQueryFilter(replaced, analyzer, expression, types, scoreField, limit, literal, getMinScore());
  }

  @Override
  protected Object[] params()
  {
    return new Object[]{field, analyzer, expression, types, scoreField, limit, literal, minScore};
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
        Pair<String, String> luceneField = Preconditions.checkNotNull(
            Lucenes.findLuceneField(field, column, TextIndexingStrategy.TYPE_NAME, JsonIndexingStrategy.TYPE_NAME),
            "cannot find lucene field name in [%s:%s]", column.getName(), column.getColumnDescs().keySet()
        );
        LuceneIndex lucene = column.getExternalIndex(LuceneIndex.class).get();
        try {
          final Query query;
          if (literal) {
            // Analyze the keyword and build a phrase (single token -> TermQuery), bypassing the query
            // parser: keyword chars are never interpreted as syntax, and a keyword that analyzes to no
            // tokens yields MatchNoDocs rather than a match-all.
            Query phrase = new QueryBuilder(Lucenes.createAnalyzer(analyzer))
                .createPhraseQuery(luceneField.getKey(), expression);
            query = phrase == null ? new MatchNoDocsQuery(expression) : phrase;
          } else {
            StandardQueryParser parser = new StandardQueryParser(Lucenes.createAnalyzer(analyzer));
            parser.setAllowLeadingWildcard(true);   // permit *term* substring queries
            parser.setFuzzyPrefixLength(FUZZY_PREFIX_LENGTH);   // guard against fuzzy term-dict scan blowup (see const)
            Map<String, PointsConfig> configMap = Lucenes.asPointConfig(types);
            if (!configMap.isEmpty()) {
              parser.setPointsConfigMap(configMap);
            }
            query = parser.parse(expression, luceneField.getKey());
          }
          return lucene.filterFor(query, context, scoreField, limit, minScore);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
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
           (limit == 0 ? "" : ", limit=" + limit) +
           (literal ? ", literal=true" : "") +
           (Float.isNaN(minScore) ? "" : ", minScore=" + minScore) +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, analyzer, expression, types, scoreField, limit, literal, minScore);
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
    if (limit != that.limit) {
      return false;
    }
    if (literal != that.literal) {
      return false;
    }
    if (Float.compare(minScore, that.minScore) != 0) {
      return false;
    }

    return true;
  }
}
