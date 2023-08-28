/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.rel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.UTF8Bytes;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.CachingSegmentWalker;
import io.druid.query.DummyQuery;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.server.QueryLifecycleFactory;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.logging.log4j.util.Strings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

public class QueryMaker
{
  private static final Logger LOG = new Logger(QueryMaker.class);

  private final QueryLifecycleFactory lifecycleFactory;
  private final QuerySegmentWalker segmentWalker;
  private final PlannerContext plannerContext;
  private final Map<String, Double> selectivities = Maps.newHashMap();
  private final Map<String, Double> cardinalities = Maps.newHashMap();

  public QueryMaker(
      final QueryLifecycleFactory lifecycleFactory,
      final QuerySegmentWalker segmentWalker,
      final PlannerContext plannerContext
  )
  {
    this.lifecycleFactory = lifecycleFactory;
    this.segmentWalker = segmentWalker;
    this.plannerContext = plannerContext;
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public ObjectMapper getJsonMapper()
  {
    return plannerContext.getObjectMapper();
  }

  public QuerySegmentWalker getSegmentWalker()
  {
    return segmentWalker;
  }

  public Sequence<Object[]> prepareAndRun(final DruidQuery druidQuery)
  {
    final Query query = druidQuery.getQuery();
    final Query prepared = prepareQuery(query);
    return runQuery(druidQuery, prepared);
  }

  // BrokerQueryResource, SpecificSegmentsQuerySegmentWalker, etc.
  public Query prepareQuery(Query<?> baseQuery)
  {
    QuerySegmentWalker walker = new CachingSegmentWalker(segmentWalker);
    Query prepared = QueryUtils.readPostProcessors(baseQuery, getJsonMapper());
    prepared = QueryUtils.rewriteRecursively(prepared, walker);
    prepared = QueryUtils.resolveRecursively(prepared, walker);
    if (plannerContext.getPlannerConfig().isRequireTimeCondition()) {
      Queries.iterate(prepared, query ->
        {
          if (!(query.getDataSource() instanceof QueryDataSource) && query.getQuerySegmentSpec() == null) {
            throw new CannotBuildQueryException(
                "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
            );
          }
          return query;
        }
      );
    }
    return prepared;
  }

  public Sequence<Object[]> runQuery(DruidQuery druidQuery, Query prepared)
  {
    Query query = druidQuery.getQuery();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running.. %s", toLog(plannerContext.getObjectMapper(), query));
    }

    Hook.QUERY_PLAN.run(query);   // original query

    Sequence<Object[]> sequence = coerce(druidQuery, prepared, runQuery(prepared));
    if (sequence == null) {
      throw new ISE("Cannot run query of class[%s]", prepared.getClass().getName());
    }
    return sequence;
  }

  private <T> Sequence<T> runQuery(final Query query)
  {
    return lifecycleFactory.factorize(query).runSimple(plannerContext.getAuthenticationResult());
  }

  @SuppressWarnings("unchecked")
  private Sequence<Object[]> coerce(DruidQuery druidQuery, Query schema, Sequence sequence)
  {
    if (schema instanceof DummyQuery) {
      return sequence;
    }
    Class<?> clazz = PostProcessingOperators.returns(schema);
    if (Row.class == clazz) {
      return executeRow(druidQuery, sequence);
    } else if (Map.class == clazz) {
      return executeRow(druidQuery, Sequences.map(sequence, Rows.mapToRow(null)));
    } else if (Object[].class == clazz) {
      return executeArray(druidQuery, sequence);
    } else if (schema instanceof BaseAggregationQuery) {
      return executeRow(druidQuery, sequence);
    } else if (schema instanceof TopNQuery) {
      return executeTopN(druidQuery, sequence);
    } else if (schema instanceof Query.ArrayOutputSupport) {
      return executeArray(druidQuery, ((Query.ArrayOutputSupport) schema).array(sequence));
    } else if (schema instanceof UnionAllQuery) {
      return coerce(druidQuery, ((UnionAllQuery) schema).getFirst(), sequence);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Sequence<Object[]> executeRow(DruidQuery druidQuery, Sequence<Row> sequence)
  {
    final List<String> columnNames = druidQuery.getOutputRowSignature().getColumnNames();
    final List<RelDataTypeField> fields = druidQuery.getOutputRowType().getFieldList();

    final int[] indices = fields.stream().mapToInt(field -> field.getIndex()).toArray();
    final String[] outputNames = IntStream.of(indices).mapToObj(x -> columnNames.get(x)).toArray(len -> new String[len]);
    final Function[] coercer = coerce(fields);

    return Sequences.map(
        sequence,
        row -> {
          final Object[] retVal = new Object[indices.length];
          for (int i = 0; i < retVal.length; i++) {
            retVal[indices[i]] = coercer[i].apply(row.getRaw(outputNames[i]));
          }
          return retVal;
        }
    );
  }

  @SuppressWarnings("unchecked")
  private Sequence<Object[]> executeTopN(DruidQuery druidQuery, Sequence<Result<TopNResultValue>> sequence)
  {
    final List<String> columnNames = druidQuery.getOutputRowSignature().getColumnNames();
    final List<RelDataTypeField> fields = druidQuery.getOutputRowType().getFieldList();

    final int[] indices = fields.stream().mapToInt(field -> field.getIndex()).toArray();
    final String[] outputNames = IntStream.of(indices).mapToObj(x -> columnNames.get(x)).toArray(len -> new String[len]);
    final Function[] coercer = coerce(fields);

    return Sequences.explode(
        sequence,
        result -> {
          return Sequences.simple(Lists.transform(result.getValue().getValue(), row -> {
            final Object[] retVal = new Object[indices.length];
            for (int i = 0; i < retVal.length; i++) {
              retVal[indices[i]] = coercer[i].apply(row.get(outputNames[i]));
            }
            return retVal;
          }));
        }
    );
  }

  @SuppressWarnings("unchecked")
  private <T> Sequence<Object[]> executeArray(DruidQuery druidQuery, Sequence<Object[]> sequence)
  {
    final List<RelDataTypeField> fields = druidQuery.getOutputRowType().getFieldList();
    final int[] indices = fields.stream().mapToInt(field -> field.getIndex()).toArray();
    final Function[] coercer = coerce(fields);
    if (GuavaUtils.isIdenticalIndex(indices)) {
      return Sequences.map(
          sequence,
          row -> {
            for (int i = 0; i < row.length; i++) {
              row[i] = coercer[i].apply(row[i]);
            }
            return row;
          }
      );
    }
    return Sequences.map(
        sequence,
        row -> {
          final Object[] retVal = new Object[indices.length];
          for (int i = 0; i < retVal.length; i++) {
            retVal[indices[i]] = coercer[i].apply(row[i]);
          }
          return retVal;
        }
    );
  }

  public Function[] coerce(List<RelDataTypeField> fields)
  {
    return fields.stream().map(
        field -> coerce(field.getType(), plannerContext.getTimeZone())).toArray(len -> new Function[len]
    );
  }

  public static Object coerece(Object literal, RelDataType dataType)
  {
    return coerce(dataType, DateTimeZone.getDefault()).apply(literal);
  }

  private static Function<Object, Object> coerce(final RelDataType dataType, final DateTimeZone timeZone)
  {
    final SqlTypeName sqlType = Calcites.getTypeName(dataType);
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return value -> {
        if (value == null) {
          return Strings.EMPTY;
        } else if (value instanceof String || value instanceof UTF8Bytes) {
          return Objects.toString(value);
        } else if (value instanceof NlsString) {
          return ((NlsString) value).getValue();
        } else if (value instanceof Number) {
          return String.valueOf(value);
        } else if (value instanceof Collection) {
          return GuavaUtils.arrayToString(((Collection) value).toArray());
        } else if (value instanceof Object[]) {
          return GuavaUtils.arrayToString((Object[]) value);
        } else {
          throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
        }
      };
    }

    switch (sqlType) {
      case TIMESTAMP:
        return value -> Calcites.coerceToTimestamp(value, timeZone);
      case DATE:
        return value -> Calcites.jodaToCalciteDate(coerceDateTime(value), timeZone);
      case INTEGER:
        return value -> value instanceof Number ? ((Number) value).intValue() : null;
      case BIGINT:
        return value -> value instanceof Number ? ((Number) value).longValue() : null;
      case FLOAT:
      case REAL:
        return value -> value instanceof Number ? ((Number) value).floatValue() : null;
      case DOUBLE:
        return value -> value instanceof Number ? ((Number) value).doubleValue() : null;
      case DECIMAL:
        return QueryMaker::coerceDecimal;
      case BOOLEAN:
        return value -> {
          if (value == null) {
            return null;
          } else if (value instanceof Boolean) {
            return (Boolean) value;
          } else if (value instanceof String) {
            return Evals.asBoolean(((String) value));
          } else if (value instanceof Number) {
            return Evals.asBoolean((Number) value);
          } else {
            throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
          }
        };
    }
    // for test.. I'm lazy (todo)
    return v -> v instanceof HyperLogLogCollector ? v.getClass().getName() : v;
  }

  private static DateTime coerceDateTime(Object value)
  {
    if (value instanceof DateTime) {
      return (DateTime) value;
    } else if (value instanceof Number) {
      return DateTimes.utc(((Number) value).longValue());
    } else if (value instanceof Calendar) {
      return new DateTime(value);
    } else {
      Long timestamp = Rows.parseLong(value, null);
      if (timestamp != null) {
        return DateTimes.utc(timestamp);
      }
      try {
        return new DateTime(value);
      }
      catch (Exception e) {
        // failed
      }
    }
    throw new ISE("Cannot coerce[%s] to dateTime", value.getClass().getName());
  }

  private static BigDecimal coerceDecimal(Object value)
  {
    if (value == null) {
      return null;
    } else if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof String) {
      return io.druid.data.Rows.tryParseDecimal((String) value);
    } else if (value instanceof Short || value instanceof Integer) {
      return BigDecimal.valueOf(((Number) value).intValue());
    } else if (value instanceof Long) {
      return BigDecimal.valueOf((Long) value);
    } else if (value instanceof Float) {
      return BigDecimal.valueOf((Float) value);
    } else if (value instanceof Double) {
      return BigDecimal.valueOf((Double) value);
    } else {
      throw new ISE("Cannot coerce[%s] to decimal", value.getClass().getName());
    }
  }

  private static Object toLog(final ObjectMapper mapper, final Object value)
  {
    try {
      return mapper.writeValueAsString(value);
    }
    catch (Exception e) {
      return Objects.toString(value, null);
    }
  }

  public Double selectivity(RelNode rel, RexNode predicate, Function<String, Double> populator)
  {
    String key = Utils.tableName(rel.getTable()) + Objects.toString(predicate, "");
    return selectivities.computeIfAbsent(key, populator);
  }

  public Double cardinality(
      RelNode rel,
      ImmutableBitSet groupKey,
      RexNode predicate,
      Function<String, Double> populator
  )
  {
    String key = Utils.tableName(rel.getTable()) + groupKey + Objects.toString(predicate, "");
    return cardinalities.computeIfAbsent(key, populator);
  }
}
