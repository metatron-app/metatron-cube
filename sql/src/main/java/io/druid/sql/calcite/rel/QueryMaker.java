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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.common.DateTimes;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.ByteArray;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.server.QueryLifecycleFactory;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueryMaker
{
  private static final Logger LOG = new Logger(QueryMaker.class);

  private final QueryLifecycleFactory lifecycleFactory;
  private final QuerySegmentWalker segmentWalker;
  private final PlannerContext plannerContext;
  private final QueryConfig queryConfig;
  private final ObjectMapper jsonMapper;

  private final Map<ByteArray, Long> cardinalityCache = Maps.newHashMap();

  public QueryMaker(
      final QueryLifecycleFactory lifecycleFactory,
      final QuerySegmentWalker segmentWalker,
      final PlannerContext plannerContext,
      final QueryConfig queryConfig,
      final ObjectMapper jsonMapper
  )
  {
    this.lifecycleFactory = lifecycleFactory;
    this.segmentWalker = segmentWalker;
    this.plannerContext = plannerContext;
    this.queryConfig = queryConfig;
    this.jsonMapper = jsonMapper;
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
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
  public Query prepareQuery(Query<?> query)
  {
    Query prepared = QueryUtils.setQueryId(query, Preconditions.checkNotNull(query.getId()));
    prepared = QueryUtils.rewriteRecursively(prepared, segmentWalker, queryConfig);
    prepared = QueryUtils.resolveRecursively(prepared, segmentWalker);
    if (plannerContext.getPlannerConfig().isRequireTimeCondition()) {
      Queries.iterate(prepared, new IdentityFunction<Query>()
      {
        @Override
        public Query apply(Query query)
        {
          if (!(query.getDataSource() instanceof QueryDataSource) && query.getQuerySegmentSpec() == null) {
            throw new CannotBuildQueryException(
                "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
            );
          }
          return query;
        }
      });
    }
    return prepared;
  }

  public Sequence<Object[]> runQuery(DruidQuery druidQuery, Query prepared)
  {
    Query query = druidQuery.getQuery();
    LOG.info("Running.. %s", toLazyLog(jsonMapper, query));

    Hook.QUERY_PLAN.run(query);   // original query

    Sequence<Object[]> sequence = coerce(druidQuery, prepared, runQuery(prepared));
    if (sequence == null) {
      throw new ISE("Cannot run query of class[%s]", prepared.getClass().getName());
    }
    return sequence;
  }

  @SuppressWarnings("unchecked")
  private Sequence<Object[]> coerce(DruidQuery druidQuery, Query schema, Sequence sequence)
  {
    Class<?> clazz = PostProcessingOperators.returns(schema, jsonMapper);
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
  private <T> Sequence<T> runQuery(final Query query)
  {
    return lifecycleFactory.factorize(query).runSimple(plannerContext.getAuthenticationResult());
  }

  private Sequence<Object[]> executeRow(final DruidQuery druidQuery, final Sequence<Row> sequence)
  {
    final List<String> columnNames = druidQuery.getOutputRowSignature().getColumnNames();
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    return Sequences.map(
        sequence,
        new Function<Row, Object[]>()
        {
          @Override
          public Object[] apply(final Row row)
          {
            final Object[] retVal = new Object[fieldList.size()];

            for (final RelDataTypeField field : fieldList) {
              final String outputName = columnNames.get(field.getIndex());
              retVal[field.getIndex()] = coerce(row.getRaw(outputName), field.getType());
            }

            return retVal;
          }
        }
    );
  }

  private Sequence<Object[]> executeTopN(
      final DruidQuery druidQuery,
      final Sequence<Result<TopNResultValue>> seqeunce
  )
  {
    final List<String> columnNames = druidQuery.getOutputRowSignature().getColumnNames();
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    return Sequences.explode(
        seqeunce,
        new Function<Result<TopNResultValue>, Sequence<Object[]>>()
        {
          @Override
          public Sequence<Object[]> apply(final Result<TopNResultValue> result)
          {
            final List<Map<String, Object>> rows = result.getValue().getValue();
            final List<Object[]> retVals = new ArrayList<>(rows.size());

            for (Map<String, Object> row : rows) {
              final Object[] retVal = new Object[fieldList.size()];
              for (final RelDataTypeField field : fieldList) {
                final String outputName = columnNames.get(field.getIndex());
                retVal[field.getIndex()] = coerce(row.get(outputName), field.getType());
              }

              retVals.add(retVal);
            }

            return Sequences.simple(retVals);
          }
        }
    );
  }

  private <T> Sequence<Object[]> executeArray(
      final DruidQuery druidQuery,
      final Sequence<Object[]> sequence
  )
  {
    final RelDataTypeField[] fields = druidQuery.getOutputRowType().getFieldList().toArray(new RelDataTypeField[0]);
    return Sequences.map(
        sequence,
        row -> {
          final Object[] retVal = new Object[fields.length];
          for (int i = 0; i < fields.length; i++) {
            retVal[fields[i].getIndex()] = coerce(row[fields[i].getIndex()], fields[i].getType());
          }
          return retVal;
        }
    );
  }

  public static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(String.class);
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.DATE) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.INTEGER) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.BIGINT) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.FLOAT) {
      return ColumnMetaData.Rep.of(Float.class);
    } else if (sqlType == SqlTypeName.DOUBLE) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.DECIMAL) {
      return ColumnMetaData.Rep.of(BigDecimal.class);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.of(Boolean.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else {
      throw new ISE("No rep for SQL type[%s]", sqlType);
    }
  }

  private Object coerce(final Object value, final RelDataType dataType)
  {
    final SqlTypeName sqlType = dataType.getSqlTypeName();
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      if (value == null || value instanceof String) {
        return StringUtils.nullToEmpty((String) value);
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
    } else if (value == null) {
      return null;
    } else if (sqlType == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      if (value instanceof String) {
        return Evals.asBoolean(((String) value));
      } else if (value instanceof Boolean) {
        return value != null && (Boolean) value;
      } else if (value instanceof Number) {
        return Evals.asBoolean((Number) value);
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.INTEGER) {
      final Long longValue = Rows.parseLong(value, null);
      if (longValue == null) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
      return longValue.intValue();
    } else if (sqlType == SqlTypeName.BIGINT) {
      Long coercedValue = Rows.parseLong(value, null);
      if (coercedValue == null) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
      return coercedValue;
    } else if (sqlType == SqlTypeName.FLOAT) {
      Float coercedValue = Rows.parseFloat(value, null);
      if (coercedValue == null) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
      return coercedValue;
    } else if (sqlType == SqlTypeName.DECIMAL) {
      return coerceDecimal(value, sqlType);
    } else if (SqlTypeName.APPROX_TYPES.contains(sqlType)) {
      try {
        return Rows.parseDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (value instanceof HyperLogLogCollector) {
      return value.getClass().getName();  // for test.. I'm lazy
    } else {
      return value;   // return as-is... it seemed better than exception
    }
  }

  private static DateTime coerceDateTime(Object value, SqlTypeName sqlType)
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
    throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
  }

  private static BigDecimal coerceDecimal(Object value, SqlTypeName sqlType)
  {
    final BigDecimal decimal;

    if (value instanceof BigDecimal) {
      decimal = (BigDecimal) value;
    } else if (value instanceof String) {
      decimal = new BigDecimal((String) value);
    } else if (value instanceof Short || value instanceof Integer) {
      decimal = new BigDecimal(((Number) value).intValue());
    } else if (value instanceof Long) {
      decimal = new BigDecimal((Long) value);
    } else if (value instanceof Float) {
      decimal = new BigDecimal((Float) value);
    } else if (value instanceof Double) {
      decimal = new BigDecimal((Double) value);
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }
    return decimal;
  }

  private static Object toLazyLog(final ObjectMapper mapper, final Object value)
  {
    return new Object()
    {
      @Override
      public String toString()
      {
        try {
          return mapper.writeValueAsString(value);
        }
        catch (Exception e) {
          return Objects.toString(value, null);
        }
      }
    };
  }

  public long estimateCardinality(final BaseAggregationQuery query)
  {
    final byte[] key = KeyBuilder.get()
                                 .appendIntervals(query.getIntervals())
                                 .append(query.getGranularity())
                                 .append(query.getFilter())
                                 .append(query.getVirtualColumns())
                                 .append(query.getDimensions())
                                 .append(query.getGroupingSets())
                                 .build();
    return cardinalityCache.computeIfAbsent(
        ByteArray.wrap(key), k -> Queries.estimateCardinality(query, segmentWalker, queryConfig)
    );
  }
}
