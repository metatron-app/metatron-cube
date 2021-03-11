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

package io.druid.sql.calcite.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.jmx.JMXQuery;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class InformationSchema extends AbstractSchema
{
  public static final String NAME = "INFORMATION_SCHEMA";

  // SCHEMATA column names:
  public static final String CATALOG_NAME = "CATALOG_NAME";
  public static final String SCHEMA_NAME = "SCHEMA_NAME";
  public static final String SCHEMA_OWNER = "SCHEMA_OWNER";
  public static final String DEFAULT_CHARACTER_SET_CATALOG = "DEFAULT_CHARACTER_SET_CATALOG";
  public static final String DEFAULT_CHARACTER_SET_SCHEMA = "DEFAULT_CHARACTER_SET_SCHEMA";
  public static final String DEFAULT_CHARACTER_SET_NAME = "DEFAULT_CHARACTER_SET_NAME";
  public static final String SQL_PATH = "SQL_PATH";

  // COLUMNS columns, from SQL standard:
  // 1. TABLE_CATALOG
  // 2. TABLE_SCHEMA
  // 3. TABLE_NAME
  // 4. COLUMN_NAME
  // 5. ORDINAL_POSITION
  // 6. COLUMN_DEFAULT
  // 7. IS_NULLABLE
  // 8. DATA_TYPE
  // 9. CHARACTER_MAXIMUM_LENGTH
  // 10. CHARACTER_OCTET_LENGTH
  // 11. NUMERIC_PRECISION
  // 12. NUMERIC_PRECISION_RADIX
  // 13. NUMERIC_SCALE
  // 14. DATETIME_PRECISION
  // 15. INTERVAL_TYPE
  // 16. INTERVAL_PRECISION
  // 17. CHARACTER_SET_CATALOG ...

  public static final String TABLE_CATALOG = "TABLE_CATALOG";
  public static final String TABLE_SCHEMA = "TABLE_SCHEMA";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String TABLE_TYPE = "TABLE_TYPE";

  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
  public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
  public static final String IS_NULLABLE = "IS_NULLABLE";
  public static final String DATA_TYPE = "DATA_TYPE";
  public static final String DATA_TYPE_EXTENDED = "DATA_TYPE_EXTENDED";
  public static final String CHARACTER_MAXIMUM_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
  public static final String CHARACTER_OCTET_LENGTH = "CHARACTER_OCTET_LENGTH";
  public static final String NUMERIC_PRECISION = "NUMERIC_PRECISION";
  public static final String NUMERIC_PRECISION_RADIX = "NUMERIC_PRECISION_RADIX";
  public static final String NUMERIC_SCALE = "NUMERIC_SCALE";
  public static final String DATETIME_PRECISION = "DATETIME_PRECISION";
  public static final String INTERVAL_TYPE = "INTERVAL_TYPE";
  public static final String INTERVAL_PRECISION = "INTERVAL_PRECISION";

  public static final String CHARACTER_SET_NAME = "CHARACTER_SET_NAME";
  public static final String COLLATION_NAME = "COLLATION_NAME";
  public static final String JDBC_TYPE = "JDBC_TYPE";

  public static final String COLUMN_DESCS = "DESCS";

  public static final String DRUID_CATALOG = "druid";    // empty string is not recognized by tableau
  public static final String SCHEMATA_TABLE = "SCHEMATA";
  public static final String TABLES_TABLE = "TABLES";
  public static final String COLUMNS_TABLE = "COLUMNS";

  private static final String SERVERS_TABLE = "SERVERS";

  private static final RowSignature SCHEMATA_SIGNATURE = RowSignature
      .builder()
      .add(CATALOG_NAME, ValueDesc.STRING)
      .add(SCHEMA_NAME, ValueDesc.STRING)
      .add(SCHEMA_OWNER, ValueDesc.STRING)
      .add(DEFAULT_CHARACTER_SET_CATALOG, ValueDesc.STRING)
      .add(DEFAULT_CHARACTER_SET_SCHEMA, ValueDesc.STRING)
      .add(DEFAULT_CHARACTER_SET_NAME, ValueDesc.STRING)
      .add(SQL_PATH, ValueDesc.STRING)
      .build();
  private static final RowSignature TABLES_SIGNATURE = RowSignature
      .builder()
      .add(TABLE_CATALOG, ValueDesc.STRING)
      .add(TABLE_SCHEMA, ValueDesc.STRING)
      .add(TABLE_NAME, ValueDesc.STRING)
      .add(TABLE_TYPE, ValueDesc.STRING)
      .build();
  private static final RowSignature COLUMNS_SIGNATURE = RowSignature
      .builder()
      .add(TABLE_CATALOG, ValueDesc.STRING)
      .add(TABLE_SCHEMA, ValueDesc.STRING)
      .add(TABLE_NAME, ValueDesc.STRING)
      .add(COLUMN_NAME, ValueDesc.STRING)
      .add(ORDINAL_POSITION, ValueDesc.STRING)
      .add(COLUMN_DEFAULT, ValueDesc.STRING)
      .add(IS_NULLABLE, ValueDesc.STRING)
      .add(DATA_TYPE, ValueDesc.STRING)
      .add(DATA_TYPE_EXTENDED, ValueDesc.STRING)
      .add(CHARACTER_MAXIMUM_LENGTH, ValueDesc.STRING)
      .add(CHARACTER_OCTET_LENGTH, ValueDesc.STRING)
      .add(NUMERIC_PRECISION, ValueDesc.STRING)
      .add(NUMERIC_PRECISION_RADIX, ValueDesc.STRING)
      .add(NUMERIC_SCALE, ValueDesc.STRING)
      .add(DATETIME_PRECISION, ValueDesc.STRING)
      .add(CHARACTER_SET_NAME, ValueDesc.STRING)
      .add(COLLATION_NAME, ValueDesc.STRING)
      .add(JDBC_TYPE, ValueDesc.LONG)
      .add(COLUMN_DESCS, ValueDesc.STRING)
      .build();
  private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

  private static final String HOST = "host";
  private static final String TYPE = "type";
  private static final String SERVICE = "service";
  private static final String START_TIME = "startTime";
  private static final String INPUT_ARGUMENTS= "inputArguments";
  private static final String AVAILABLE_PROCESSOR = "availableProcessor";
  private static final String SYSTEM_LOAD_AVERAGE = "systemLoadAverage";
  private static final String HEAP_MAX = "heap.max";
  private static final String HEAP_USED = "heap.used";
  private static final String HEAP_COMMITTED = "heap.committed";
  private static final String NONHEAP_MAX = "non-heap.max";
  private static final String NONHEAP_USED = "non-heap.used";
  private static final String NONHEAP_COMMITTED = "non-heap.committed";
  private static final String THREAD_COUNT = "threadCount";
  private static final String PEAK_THREAD_COUNT = "peakThreadCount";
  private static final String TOTAL_STARTED_THREAD_COUNT = "totalStartedThreadCount";
  private static final String GC_COLLECTION_COUNT = "gc.collectionCount";
  private static final String GX_COLLECTION_TIME = "gc.collectionTime";

  public static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add(HOST, ValueDesc.STRING)
      .add(TYPE, ValueDesc.STRING)
      .add(SERVICE, ValueDesc.STRING)
      .add(START_TIME, ValueDesc.STRING)
      .add(INPUT_ARGUMENTS, ValueDesc.STRING)
      .add(AVAILABLE_PROCESSOR, ValueDesc.LONG)
      .add(SYSTEM_LOAD_AVERAGE, ValueDesc.DOUBLE)
      .add(HEAP_MAX, ValueDesc.LONG)
      .add(HEAP_USED, ValueDesc.LONG)
      .add(HEAP_COMMITTED, ValueDesc.LONG)
      .add(NONHEAP_MAX, ValueDesc.LONG)
      .add(NONHEAP_USED, ValueDesc.LONG)
      .add(NONHEAP_COMMITTED, ValueDesc.LONG)
      .add(THREAD_COUNT, ValueDesc.LONG)
      .add(PEAK_THREAD_COUNT, ValueDesc.LONG)
      .add(TOTAL_STARTED_THREAD_COUNT, ValueDesc.LONG)
      .add(GC_COLLECTION_COUNT, ValueDesc.MAP)
      .add(GX_COLLECTION_TIME, ValueDesc.MAP)
      .build();

  private final SchemaPlus rootSchema;
  private final QuerySegmentWalker segmentWalker;
  private final Map<String, Table> tableMap;

  @Inject
  public InformationSchema(
      final SchemaPlus rootSchema,
      final QuerySegmentWalker segmentWalker
  )
  {
    this.rootSchema = Preconditions.checkNotNull(rootSchema, "rootSchema");
    this.segmentWalker = segmentWalker;
    this.tableMap = ImmutableMap.<String, Table>of(
        SCHEMATA_TABLE, new SchemataTable(),
        TABLES_TABLE, new TablesTable(),
        COLUMNS_TABLE, new ColumnsTable(),
        SERVERS_TABLE, new ServersTable()
    );
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  class SchemataTable extends AbstractTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transform(
              new Function<String, Object[]>()
              {
                @Override
                public Object[] apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  return new Object[]{
                      DRUID_CATALOG, // CATALOG_NAME
                      subSchema.getName(), // SCHEMA_NAME
                      null, // SCHEMA_OWNER
                      null, // DEFAULT_CHARACTER_SET_CATALOG
                      null, // DEFAULT_CHARACTER_SET_SCHEMA
                      null, // DEFAULT_CHARACTER_SET_NAME
                      null  // SQL_PATH
                  };
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return SCHEMATA_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }

  class TablesTable extends AbstractTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transformAndConcat(
              new Function<String, Iterable<Object[]>>()
              {
                @Override
                public Iterable<Object[]> apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  return Iterables.filter(
                      Iterables.concat(
                          FluentIterable.from(subSchema.getTableNames()).transform(
                              new Function<String, Object[]>()
                              {
                                @Override
                                public Object[] apply(final String tableName)
                                {
                                  return new Object[]{
                                      DRUID_CATALOG, // TABLE_CATALOG
                                      schemaName, // TABLE_SCHEMA
                                      tableName, // TABLE_NAME
                                      subSchema.getTable(tableName).getJdbcTableType().toString() // TABLE_TYPE
                                  };
                                }
                              }
                          ),
                          FluentIterable.from(subSchema.getFunctionNames()).transform(
                              new Function<String, Object[]>()
                              {
                                @Override
                                public Object[] apply(final String functionName)
                                {
                                  if (getView(subSchema, functionName) != null) {
                                    return new Object[]{
                                        DRUID_CATALOG, // TABLE_CATALOG
                                        schemaName, // TABLE_SCHEMA
                                        functionName, // TABLE_NAME
                                        "VIEW" // TABLE_TYPE
                                    };
                                  } else {
                                    return null;
                                  }
                                }
                              }
                          )
                      ),
                      Predicates.notNull()
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return TABLES_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }

  class ColumnsTable extends AbstractTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transformAndConcat(
              new Function<String, Iterable<Object[]>>()
              {
                @Override
                public Iterable<Object[]> apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(TYPE_SYSTEM);

                  return Iterables.concat(
                      Iterables.filter(
                          Iterables.concat(
                              FluentIterable.from(subSchema.getTableNames()).transform(
                                  new Function<String, Iterable<Object[]>>()
                                  {
                                    @Override
                                    public Iterable<Object[]> apply(final String tableName)
                                    {
                                      return generateColumnMetadata(
                                          schemaName,
                                          tableName,
                                          subSchema.getTable(tableName),
                                          typeFactory
                                      );
                                    }
                                  }
                              ),
                              FluentIterable.from(subSchema.getFunctionNames()).transform(
                                  new Function<String, Iterable<Object[]>>()
                                  {
                                    @Override
                                    public Iterable<Object[]> apply(final String functionName)
                                    {
                                      final TableMacro viewMacro = getView(subSchema, functionName);
                                      if (viewMacro == null) {
                                        return null;
                                      }

                                      return generateColumnMetadata(
                                          schemaName,
                                          functionName,
                                          viewMacro.apply(ImmutableList.of()),
                                          typeFactory
                                      );
                                    }
                                  }
                              )
                          ),
                          Predicates.notNull()
                      )
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return COLUMNS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Nullable
    private Iterable<Object[]> generateColumnMetadata(
        final String schemaName,
        final String tableName,
        final Table table,
        final RelDataTypeFactory typeFactory
    )
    {
      if (table == null) {
        return null;
      }

      return FluentIterable
          .from(table.getRowType(typeFactory).getFieldList())
          .transform(
              new Function<RelDataTypeField, Object[]>()
              {
                @Override
                public Object[] apply(final RelDataTypeField field)
                {
                  final String name = field.getName();
                  final RelDataType type = field.getType();
                  boolean isNumeric = SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName());
                  boolean isCharacter = SqlTypeName.CHAR_TYPES.contains(type.getSqlTypeName());
                  boolean isDateTime = SqlTypeName.DATETIME_TYPES.contains(type.getSqlTypeName());
                  String descriptor = null;
                  if (table instanceof DruidTable) {
                    descriptor = Objects.toString(((DruidTable) table).getDescriptors().get(name), "");
                  }
                  return new Object[]{
                      DRUID_CATALOG, // TABLE_CATALOG
                      schemaName, // TABLE_SCHEMA
                      tableName, // TABLE_NAME
                      name, // COLUMN_NAME
                      String.valueOf(field.getIndex()), // ORDINAL_POSITION
                      "", // COLUMN_DEFAULT
                      type.isNullable() ? "YES" : "NO", // IS_NULLABLE
                      type.getSqlTypeName().toString(), // DATA_TYPE
                      type.getFullTypeString(), // DATA_TYPE_EXTENDED
                      null, // CHARACTER_MAXIMUM_LENGTH
                      null, // CHARACTER_OCTET_LENGTH
                      isNumeric ? String.valueOf(type.getPrecision()) : null, // NUMERIC_PRECISION
                      isNumeric ? "10" : null, // NUMERIC_PRECISION_RADIX
                      isNumeric ? String.valueOf(type.getScale()) : null, // NUMERIC_SCALE
                      isDateTime ? String.valueOf(type.getPrecision()) : null, // DATETIME_PRECISION
                      isCharacter ? type.getCharset().name() : null, // CHARACTER_SET_NAME
                      isCharacter ? type.getCollation().getCollationName() : null, // COLLATION_NAME
                      type.getSqlTypeName().getJdbcOrdinal(), // JDBC_TYPE (Druid extension)
                      descriptor
                  };
                }
              }
          );
    }
  }

  class ServersTable extends AbstractTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      JMXQuery query = new JMXQuery(
          null, null, null, false, ImmutableMap.of(Query.QUERYID, UUID.randomUUID().toString())
      );
      Iterable<Object[]> results = Iterables.concat(Iterables.transform(
          Sequences.toList(QueryRunners.run(query, segmentWalker)),
          new Function<Map<String, Object>, Iterable<Object[]>>()
          {
            private final List<String> rowOrder = SERVERS_SIGNATURE.getColumnNames();

            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Object[]> apply(Map<String, Object> input)
            {
              List<Object[]> values = Lists.newArrayList();
              for (Map.Entry<String, Object> entry : input.entrySet()) {
                Map<String, Object> stats = (Map<String, Object>) entry.getValue();
                final Object[] array = new Object[rowOrder.size()];
                array[0] = entry.getKey();
                for (int i = 1; i < array.length; i++) {
                  array[i] = stats.get(rowOrder.get(i));
                }
                values.add(array);
              }
              return values;
            }
          }
      ));
      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return SERVERS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_VIEW;
    }
  }

  /**
   * Return a view macro that may or may not be defined in a certain schema. If it's not defined, returns null.
   *
   * @param schemaPlus   schema
   * @param functionName function name
   *
   * @return view, or null
   */
  @Nullable
  private static TableMacro getView(final SchemaPlus schemaPlus, final String functionName)
  {
    // Look for a zero-arg function that is also a TableMacro. The returned value
    // is never null so we don't need to check for that.
    final Collection<org.apache.calcite.schema.Function> functions =
        schemaPlus.getFunctions(functionName);

    for (org.apache.calcite.schema.Function function : functions) {
      if (function.getParameters().isEmpty() && function instanceof TableMacro) {
        return (TableMacro) function;
      }
    }

    return null;
  }
}
