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

package io.druid.firehose.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.codehaus.plexus.util.CollectionUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;

public class JDBCFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(JDBCFirehoseFactory.class);
  private final String DEFAULT_NULLSTRING = "";
  private final float DEFAULT_NULLNUMERIC = 0;

  @JsonProperty
  private final JDBCConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final String query;
  @JsonProperty
  private final List<String> columns;
  @JsonProperty
  private final String nullString;
  @JsonProperty
  private final Float nullNumeric;
  @JsonProperty
  private final Integer fetchSize;

  @JsonCreator
  public JDBCFirehoseFactory(
      @JsonProperty(value = "connectorConfig", required = true)
      final JDBCConnectorConfig connectorConfig,
      @JsonProperty(value = "table", required = true)
      final String table,
      @JsonProperty(value = "query", required = false)
      final String query,
      @JsonProperty(value = "nullString", required = false)
      final String nullString,
      @JsonProperty(value = "nullNumeric", required = false)
      final Float nullNumeric,
      @JsonProperty(value = "fetchSize", required = false)
      final Integer fetchSize,
      @Nullable @JsonProperty(value = "columns", required = false)
      final List<String> columns
      )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    Preconditions.checkArgument(table != null || (query != null && columns != null),
        "one of table or query should be set");
    this.table = table;
    this.query = query;
    this.nullString = nullString == null ? DEFAULT_NULLSTRING : nullString;
    this.nullNumeric = nullNumeric == null ? DEFAULT_NULLNUMERIC : nullNumeric;
    this.fetchSize = fetchSize;
    this.columns = columns;
  }

  public JDBCConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  public String getTable()
  {
    return table;
  }

  public List<String> getColumns()
  {
    return columns;
  }

  public String getQuery()
  {
    return query;
  }

  public String getNullString()
  {
    return nullString;
  }

  public Float getNullNumeric()
  {
    return nullNumeric;
  }

  public Integer getFetchSize()
  {
    return fetchSize;
  }

  @Override
  public Firehose connect(final InputRowParser parser) throws IOException, ParseException, IllegalArgumentException
  {
    if (columns != null) {
      verifyParserSpec(parser, columns);
    }

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    dataSource.setUrl(connectorConfig.getConnectURI());

    dataSource.setDriverClassLoader(getClass().getClassLoader());
    if (!StringUtils.isEmpty(connectorConfig.getDriverClass())) {
      dataSource.setDriverClassName(connectorConfig.getDriverClass());
    }

    final Handle handle = new DBI(dataSource).open();

    final String query = (table != null) ? makeQuery(columns) : this.query;

    Query<Map<String, Object>> dbiQuery = handle.createQuery(query);
    if (fetchSize != null) {
      dbiQuery = dbiQuery.setFetchSize(fetchSize);
    }

    final ResultIterator<InputRow> rowIterator = dbiQuery
        .map(
            new ResultSetMapper<InputRow>()
            {
              List<String> queryColumns = (columns == null) ? Lists.<String>newArrayList(): columns;
              List<Boolean> columnIsNumeric = Lists.newArrayList();

              @Override
              public InputRow map(
                  final int index,
                  final ResultSet r,
                  final StatementContext ctx
              ) throws SQLException
              {
                try {
                  if (queryColumns.size() == 0) {
                    ResultSetMetaData metadata = r.getMetaData();
                    for (int idx = 1; idx <= metadata.getColumnCount(); idx++) {
                      queryColumns.add(metadata.getColumnName(idx));
                    }
                    Preconditions.checkArgument(queryColumns.size() > 0,
                        String.format("No column in table [%s]", table));
                    verifyParserSpec(parser, queryColumns);
                  }
                  if (columnIsNumeric.size() == 0) {
                    ResultSetMetaData metadata = r.getMetaData();
                    Preconditions.checkArgument(metadata.getColumnCount() >= queryColumns.size(),
                        String.format("number of column names [%d] exceeds the actual number of returning column values [%d]",
                            queryColumns.size(), metadata.getColumnCount()));
                    columnIsNumeric.add(false); // dummy to make start index to 1
                    for (int idx = 1; idx <= metadata.getColumnCount(); idx++) {
                      boolean isNumeric = false;
                      int type = metadata.getColumnType(idx);
                      switch(type) {
                        case BIGINT:
                        case DECIMAL:
                        case DOUBLE:
                        case FLOAT:
                        case INTEGER:
                        case NUMERIC:
                        case SMALLINT:
                        case TINYINT:
                          isNumeric = true;
                          break;
                      }
                      columnIsNumeric.add(isNumeric);
                    }
                  }
                  final Map<String, Object> columnMap = Maps.newHashMap();
                  int columnIdx = 1;
                  for (String column: queryColumns) {
                    Object objToPut = null;
                    if (table != null) {
                      objToPut = r.getObject(column);
                    } else {
                      objToPut = r.getObject(columnIdx);
                    }
                    columnMap.put(column,
                        objToPut == null ?
                            (columnIsNumeric.get(columnIdx) ? nullNumeric: nullString)
                            : objToPut);

                    columnIdx++;
                  }
                  return parser.parse(columnMap);

                } catch(IllegalArgumentException e) {
                  throw new SQLException(e);
                }
              }
            }
        ).iterator();

    return new Firehose() {
      @Override
      public boolean hasMore()
      {
        return rowIterator.hasNext();
      }

      @Override
      public InputRow nextRow()
      {
        return rowIterator.next();
      }

      @Override
      public Runnable commit()
      {
        return null;
      }

      @Override
      public void close() throws IOException
      {
        rowIterator.close();
        handle.close();
      }
    };
  }

  @Override
  public String toString()
  {
    return String.format(
        "JDBCFirehoseFactory= { connectorConfig = { %s }, table = %s, columns = %s}",
        connectorConfig.toString(),
        table,
        columns != null ? StringUtils.join(columns, ',') : "null"
    );
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

    JDBCFirehoseFactory that = (JDBCFirehoseFactory)o;

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (columns.size() != that.columns.size()) {
      return false;
    } else {
      return CollectionUtils.subtract(columns, that.columns).size() == 0;
    }
  }

  private void verifyParserSpec(InputRowParser parser, List<String> storedColumns) throws IllegalArgumentException
  {
    String tsColumn = parser.getTimestampSpec().getTimestampColumn();
    Preconditions.checkArgument(tsColumn == null || storedColumns.contains(tsColumn),
        String.format("timestamp column %s does not exist in table %s", tsColumn, table));

    for (DimensionSchema dim : parser.getDimensionsSpec().getDimensions())
    {
      Preconditions.checkArgument(storedColumns.contains(dim.getName()),
          String.format("dimension column %s does not exist in table %s", dim, table));
    }
  }

  private String makeQuery(List<String> requiredFields)
  {
    if (requiredFields == null)
    {
      return new StringBuilder("SELECT *  FROM ").append(table).toString();
    }
    return new StringBuilder("SELECT ").append(StringUtils.join(requiredFields, ','))
                                       .append(" from ")
                                       .append(table)
                                       .toString();
  }
}
