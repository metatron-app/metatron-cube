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

package io.druid.sql.calcite.rel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.data.Rows;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class QueryMaker
{
  private static final Logger LOG = new Logger(QueryMaker.class);

  private final QuerySegmentWalker segmentWalker;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;

  public QueryMaker(
      final QuerySegmentWalker segmentWalker,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper
  )
  {
    this.segmentWalker = segmentWalker;
    this.plannerContext = plannerContext;
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

  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    Query query = druidQuery.getQuery();
    if (query.getId() == null) {
      query = query.withId(UUID.randomUUID().toString());
    }
    LOG.info("Running query : %s", query);
    if (query instanceof TimeseriesQuery) {
      return executeTimeseries(druidQuery, (TimeseriesQuery) query);
    } else if (query instanceof TopNQuery) {
      return executeTopN(druidQuery, (TopNQuery) query);
    } else if (query instanceof GroupByQuery) {
      return executeGroupBy(druidQuery, (GroupByQuery) query);
    } else if (query instanceof StreamRawQuery) {
      return executeScan(druidQuery, (StreamRawQuery) query);
    } else if (query instanceof SelectQuery) {
      return executeSelect(druidQuery, (SelectQuery) query);
    } else {
      throw new ISE("Cannot run query of class[%s]", query.getClass().getName());
    }
  }

  private Sequence<Object[]> executeScan(
      final DruidQuery druidQuery,
      final StreamRawQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    final RowSignature outputRowSignature = druidQuery.getOutputRowSignature();

    // SQL row column index -> Scan query column index
    final int[] columnMapping = new int[outputRowSignature.getRowOrder().size()];

    return Sequences.concat(
        Sequences.map(
            runQuery(query),
            scanResult -> {
              Schema schema = scanResult.getSchema();
              List<String> columnNames = schema.getColumnNames();
              final Map<String, Integer> scanColumnOrder = Maps.newHashMap();

              for (int i = 0; i < columnNames.size(); i++) {
                scanColumnOrder.put(columnNames.get(i), i);
              }

              for (int i = 0; i < outputRowSignature.getRowOrder().size(); i++) {
                String columnName = outputRowSignature.getRowOrder().get(i);
                if (columnName.equals(Column.TIME_COLUMN_NAME)) {
                  columnName = EventHolder.timestampKey;  // fuck
                }
                final Integer index = scanColumnOrder.get(columnName);
                columnMapping[i] = index == null ? -1 : index;
              }

              final List<Object[]> retVals = new ArrayList<>();
              for (Object[] row : scanResult.getRows()) {
                final Object[] retVal = new Object[fieldList.size()];
                for (RelDataTypeField field : fieldList) {
                  int index = columnMapping[field.getIndex()];
                  if (index < 0) {
                    continue;
                  }
                  retVal[field.getIndex()] = coerce(row[index], field.getType().getSqlTypeName());
                }
                retVals.add(retVal);
              }
              return Sequences.<Object[]>simple(retVals);
            }
        )
    );
  }

  private Sequence<Object[]> executeSelect(
      final DruidQuery druidQuery,
      final SelectQuery baseQuery
  )
  {
    Preconditions.checkState(druidQuery.getGrouping() == null, "grouping must be null");

    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();
    final Integer limit = druidQuery.getLimitSpec() != null ? druidQuery.getLimitSpec().getLimit() : null;
    final RowSignature outputRowSignature = druidQuery.getOutputRowSignature();

    // Select is paginated, we need to make multiple queries.
    final Sequence<Sequence<Object[]>> sequenceOfSequences = Sequences.simple(
        new Iterable<Sequence<Object[]>>()
        {
          @Override
          public Iterator<Sequence<Object[]>> iterator()
          {
            final AtomicBoolean morePages = new AtomicBoolean(true);
            final AtomicReference<Map<String, Integer>> pagingIdentifiers = new AtomicReference<>();
            final AtomicLong rowsRead = new AtomicLong();

            // Each Sequence<Object[]> is one page.
            return new Iterator<Sequence<Object[]>>()
            {
              @Override
              public boolean hasNext()
              {
                return morePages.get();
              }

              @Override
              public Sequence<Object[]> next()
              {
                final SelectQuery queryWithPagination = baseQuery.withPagingSpec(
                    new PagingSpec(
                        pagingIdentifiers.get(),
                        plannerContext.getPlannerConfig().getSelectThreshold(),
                        true
                    )
                );

                morePages.set(false);
                final AtomicBoolean gotResult = new AtomicBoolean();

                return Sequences.concat(
                    Sequences.map(
                        runQuery(queryWithPagination),
                        new Function<Result<SelectResultValue>, Sequence<Object[]>>()
                        {
                          @Override
                          public Sequence<Object[]> apply(final Result<SelectResultValue> result)
                          {
                            if (!gotResult.compareAndSet(false, true)) {
                              throw new ISE("WTF?! Expected single result from Select query but got multiple!");
                            }

                            pagingIdentifiers.set(result.getValue().getPagingIdentifiers());
                            final List<Object[]> retVals = new ArrayList<>();
                            for (EventHolder holder : result.getValue().getEvents()) {
                              morePages.set(true);
                              final Map<String, Object> map = holder.getEvent();
                              final Object[] retVal = new Object[fieldList.size()];
                              for (RelDataTypeField field : fieldList) {
                                final String outputName = outputRowSignature.getRowOrder().get(field.getIndex());
                                if (outputName.equals(Column.TIME_COLUMN_NAME)) {
                                  retVal[field.getIndex()] = coerce(
                                      holder.getTimestamp(),
                                      field.getType().getSqlTypeName()
                                  );
                                } else {
                                  retVal[field.getIndex()] = coerce(
                                      map.get(outputName),
                                      field.getType().getSqlTypeName()
                                  );
                                }
                              }
                              if (limit == null || rowsRead.incrementAndGet() <= limit) {
                                retVals.add(retVal);
                              } else {
                                morePages.set(false);
                                return Sequences.simple(retVals);
                              }
                            }

                            return Sequences.simple(retVals);
                          }
                        }
                    )
                );
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );

    return Sequences.concat(sequenceOfSequences);
  }

  @SuppressWarnings("unchecked")
  private <T> Sequence<T> runQuery(final Query<T> query)
  {
    Hook.QUERY_PLAN.run(query);
    return query.run(segmentWalker, Maps.newHashMap());
  }

  private Sequence<Object[]> executeTimeseries(
      final DruidQuery druidQuery,
      final TimeseriesQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();
    final String timeOutputName = druidQuery.getGrouping().getDimensions().isEmpty()
                                  ? null
                                  : Iterables.getOnlyElement(druidQuery.getGrouping().getDimensions())
                                             .getOutputName();

    final List<String> rowOrder = druidQuery.getOutputRowSignature().getRowOrder();

    return Sequences.map(
        runQuery(query),
        new Function<Result<TimeseriesResultValue>, Object[]>()
        {
          @Override
          public Object[] apply(final Result<TimeseriesResultValue> result)
          {
            final Map<String, Object> row = result.getValue().getBaseObject();
            final Object[] retVal = new Object[fieldList.size()];

            for (final RelDataTypeField field : fieldList) {
              final String outputName = rowOrder.get(field.getIndex());
              if (outputName.equals(timeOutputName)) {
                retVal[field.getIndex()] = coerce(result.getTimestamp(), field.getType().getSqlTypeName());
              } else {
                retVal[field.getIndex()] = coerce(row.get(outputName), field.getType().getSqlTypeName());
              }
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

    return Sequences.concat(
        Sequences.map(
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
        )
    );
  }

  private Sequence<Object[]> executeGroupBy(
      final DruidQuery druidQuery,
      final GroupByQuery query
  )
  {
    final List<RelDataTypeField> fieldList = druidQuery.getOutputRowType().getFieldList();

    return Sequences.map(
        runQuery(query),
        new Function<Row, Object[]>()
        {
          @Override
          public Object[] apply(final Row row)
          {
            final Object[] retVal = new Object[fieldList.size()];
            for (RelDataTypeField field : fieldList) {
              retVal[field.getIndex()] = coerce(
                  row.getRaw(druidQuery.getOutputRowSignature().getRowOrder().get(field.getIndex())),
                  field.getType().getSqlTypeName()
              );
            }
            return retVal;
          }
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
        coercedValue = Strings.nullToEmpty((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else if (value instanceof Number) {
        coercedValue = String.valueOf(value);
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