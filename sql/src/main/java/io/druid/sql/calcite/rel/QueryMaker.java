/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.math.expr.Evals;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class QueryMaker
{
  private static final Logger LOG = new Logger(QueryMaker.class);

  private final QuerySegmentWalker segmentWalker;
  private final PlannerContext plannerContext;
  private final QueryConfig queryConfig;
  private final ObjectMapper jsonMapper;

  public QueryMaker(
      final QuerySegmentWalker segmentWalker,
      final PlannerContext plannerContext,
      final QueryConfig queryConfig,
      final ObjectMapper jsonMapper
  )
  {
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

  public QueryConfig getQueryConfig()
  {
    return queryConfig;
  }

  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    final Query query = druidQuery.getQuery();
    try {
      LOG.info("Running.. %s", jsonMapper.writeValueAsString(query));
    }
    catch (JsonProcessingException e) {
      LOG.info("Running.. %s", query);
    }

    final Query prepared = prepareQuery(query);
    if (plannerContext.getPlannerConfig().isRequireTimeCondition()) {
      Queries.iterate(prepared, new IdentityFunction<Query>()
      {
        @Override
        public Query apply(Query input)
        {
          if (!(prepared.getDataSource() instanceof QueryDataSource) &&
              prepared.getQuerySegmentSpec() == null) {
            throw new CannotBuildQueryException(
                "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
            );
          }
          return input;
        }
      });
    }

    Hook.QUERY_PLAN.run(query);   // original query

    if (prepared instanceof BaseAggregationQuery) {
      return executeAggregation(druidQuery, (BaseAggregationQuery) prepared);
    } else if (prepared instanceof TopNQuery) {
      return executeTopN(druidQuery, (TopNQuery) prepared);
    } else if (prepared instanceof Query.ArrayOutputSupport) {
      return executeArray(druidQuery, (Query.ArrayOutputSupport) prepared);
    } else {
      throw new ISE("Cannot run query of class[%s]", prepared.getClass().getName());
    }
  }

  // BrokerQueryResource, SpecificSegmentsQuerySegmentWalker, etc.
  public Query prepareQuery(Query<?> query)
  {
    String queryId = query.getId() == null ? UUID.randomUUID().toString() : query.getId();
    query = QueryUtils.setQueryId(query, queryId);
    query = QueryUtils.rewriteRecursively(query, segmentWalker, queryConfig);
    query = QueryUtils.resolveRecursively(query, segmentWalker);
    return query;
  }

  @SuppressWarnings("unchecked")
  private <T> Sequence<T> runQuery(final Query query)
  {
    return query.run(segmentWalker, Maps.newHashMap());
  }

  private Sequence<Object[]> executeAggregation(
      final DruidQuery druidQuery,
      final BaseAggregationQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    final List<String> rowOrder = druidQuery.getOutputRowSignature().getRowOrder();

    return Sequences.map(
        runQuery(query),
        new Function<Row, Object[]>()
        {
          @Override
          public Object[] apply(final Row row)
          {
            final Object[] retVal = new Object[fieldList.size()];

            for (final RelDataTypeField field : fieldList) {
              final String outputName = rowOrder.get(field.getIndex());
              retVal[field.getIndex()] = coerce(row.getRaw(outputName), field.getType().getSqlTypeName());
            }

            return retVal;
          }
        }
    );
  }

  private Sequence<Object[]> executeTopN(
      final DruidQuery druidQuery,
      final TopNQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    return Sequences.explode(
        runQuery(query),
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
                final String outputName = druidQuery.getOutputRowSignature().getRowOrder().get(field.getIndex());
                retVal[field.getIndex()] = coerce(row.get(outputName), field.getType().getSqlTypeName());
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
      final Query.ArrayOutputSupport<?> query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    final RowSignature outputRowSignature = druidQuery.getOutputRowSignature();

    // SQL row column index -> Scan query column index
    final int[] columnMapping = new int[outputRowSignature.getRowOrder().size()];

    final List<String> columnNames = query.estimatedOutputColumns();
    final Map<String, Integer> scanColumnOrder = Maps.newHashMap();

    for (int i = 0; i < columnNames.size(); i++) {
      scanColumnOrder.put(columnNames.get(i), i);
    }

    for (int i = 0; i < outputRowSignature.getRowOrder().size(); i++) {
      String columnName = outputRowSignature.getRowOrder().get(i);
      Integer index = scanColumnOrder.get(columnName);
      columnMapping[i] = index == null ? -1 : index;
    }
    return Sequences.map(
        query.array(runQuery(query)),
        row -> {
          final Object[] retVal = new Object[fieldList.size()];
          for (RelDataTypeField field : fieldList) {
            int index = columnMapping[field.getIndex()];
            if (index < 0) {
              continue;
            }
            retVal[field.getIndex()] = coerce(row[index], field.getType().getSqlTypeName());
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
    } else if (sqlType == SqlTypeName.DOUBLE || sqlType == SqlTypeName.DECIMAL) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.of(Boolean.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else {
      throw new ISE("No rep for SQL type[%s]", sqlType);
    }
  }

  private Object coerce(final Object value, final SqlTypeName sqlType)
  {
    final Object coercedValue;

    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      if (value == null || value instanceof String) {
        coercedValue = StringUtils.nullToEmpty((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else if (value instanceof Number) {
        coercedValue = String.valueOf(value);
      } else if (value instanceof Collection) {
        // Iterate through the collection, coercing each value. Useful for handling selects of multi-value dimensions.
        final List<String> valueStrings = ((Collection<?>) value).stream()
                                                                 .map(v -> (String) coerce(v, sqlType))
                                                                 .collect(Collectors.toList());

        try {
          coercedValue = jsonMapper.writeValueAsString(valueStrings);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlType == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      if (value instanceof String) {
        coercedValue = Evals.asBoolean(((String) value));
      } else if (value instanceof Number) {
        coercedValue = Evals.asBoolean(((Number) value).longValue());
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.BIGINT) {
      try {
        coercedValue = Rows.parseLong(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.FLOAT) {
      try {
        coercedValue = Rows.parseFloat(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlType)) {
      try {
        coercedValue = Rows.parseDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.OTHER) {
      // Complex type got out somehow.
      coercedValue = value.getClass().getName();
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
  }

  private static DateTime coerceDateTime(Object value, SqlTypeName sqlType)
  {
    final DateTime dateTime;

    if (value instanceof Number) {
      dateTime = DateTimes.utc(((Number) value).longValue());
    } else if (value instanceof String) {
      dateTime = DateTimes.utc(Long.parseLong((String) value));
    } else if (value instanceof DateTime) {
      dateTime = (DateTime) value;
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }
    return dateTime;
  }
}
