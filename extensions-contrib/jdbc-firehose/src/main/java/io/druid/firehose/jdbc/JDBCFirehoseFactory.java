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

package io.druid.firehose.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.lang.StringUtils;
import org.codehaus.plexus.util.CollectionUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class JDBCFirehoseFactory implements FirehoseFactory<MapInputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(JDBCFirehoseFactory.class);

  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final List<String> columns;

  @JsonCreator
  public JDBCFirehoseFactory(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true)
      final String table,
      @Nullable @JsonProperty(value = "columns", required = false)
      final List<String> columns
      )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    this.columns = columns;
  }

  public MetadataStorageConnectorConfig getConnectorConfig()
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

  @Override
  public Firehose connect(final MapInputRowParser parser) throws IOException, ParseException, IllegalArgumentException
  {
    if (columns != null) {
      verifyParserSpec(parser.getParseSpec(), columns);
    }

    final Handle handle = new DBI(
        connectorConfig.getConnectURI(),
        connectorConfig.getUser(),
        connectorConfig.getPassword()
    ).open();

    final String query = makeQuery(columns);

    final ResultIterator<InputRow> rowIterator = handle
        .createQuery(query)
        .map(
            new ResultSetMapper<InputRow>()
            {
              List<String> queryColumns = (columns == null) ? Lists.<String>newArrayList(): columns;

              @Override
              public InputRow map(
                  final int index,
                  final ResultSet r,
                  final StatementContext ctx
              ) throws SQLException
              {
                try {
                  if (queryColumns.size() == 0)
                  {
                    ResultSetMetaData metadata = r.getMetaData();
                    for (int idx = 1; idx <= metadata.getColumnCount(); idx++)
                    {
                      queryColumns.add(metadata.getColumnName(idx));
                    }
                    Preconditions.checkArgument(queryColumns.size() > 0,
                        String.format("No column in table [%s]", table));
                    verifyParserSpec(parser.getParseSpec(), queryColumns);
                  }
                  ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder();
                  for (String column: queryColumns) {
                    builder.put(column, r.getObject(column));
                  }
                  return parser.parse(builder.build());

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

  private void verifyParserSpec(ParseSpec parseSpec, List<String> storedColumns) throws IllegalArgumentException
  {
    String tsColumn = parseSpec.getTimestampSpec().getTimestampColumn();
    Preconditions.checkArgument(storedColumns.contains(tsColumn),
        String.format("timestamp column %s does not exist in table %s", tsColumn, table));

    for (DimensionSchema dim: parseSpec.getDimensionsSpec().getDimensions())
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
