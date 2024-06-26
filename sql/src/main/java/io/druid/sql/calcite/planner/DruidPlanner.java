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

package io.druid.sql.calcite.planner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.BrokerServerView;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.ForwardConstants;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryException;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.load.LoadQuery;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.indexing.DataSchema;
import io.druid.server.FileLoadSpec;
import io.druid.server.TypeStringResolver;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.ddl.SqlCreateTable;
import io.druid.sql.calcite.ddl.SqlDescPath;
import io.druid.sql.calcite.ddl.SqlDropTable;
import io.druid.sql.calcite.ddl.SqlInsertDirectory;
import io.druid.sql.calcite.ddl.SqlLoadTable;
import io.druid.sql.calcite.rel.DruidConvention;
import io.druid.sql.calcite.rel.DruidQuery;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.timeline.DataSegment;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidPlanner implements Closeable, ForwardConstants
{
  private static final Logger LOG = new Logger(DruidPlanner.class);

  private final QueryMaker queryMaker;
  private final Planner planner;
  private final PlannerContext plannerContext;

  public DruidPlanner(
      Planner planner,
      PlannerContext plannerContext,
      QueryMaker queryMaker
  )
  {
    this.planner = planner;
    this.plannerContext = plannerContext;
    this.queryMaker = queryMaker;
  }

  public PlannerResult plan(String sql, BrokerServerView brokerServerView)
      throws SqlParseException, ValidationException, RelConversionException
  {
    return plan(sql, brokerServerView, -1);
  }

  public PlannerResult plan(final String sql, final BrokerServerView brokerServerView, long maxRowCount)
      throws SqlParseException, ValidationException, RelConversionException
  {
    final SqlNode source = planner.parse(sql);
    if (source.getKind() == SqlKind.DROP_TABLE) {
      return handleDropTable((SqlDropTable) source, brokerServerView);
    }
    if (source.getKind() == SqlKind.CREATE_TABLE && source instanceof SqlLoadTable) {
      return handleLoadTable((SqlLoadTable) source, brokerServerView);
    }
    if (source instanceof SqlDescPath) {
      return handleDescPath((SqlDescPath) source);
    }
    SqlNode target = source;
    if (target.getKind() == SqlKind.EXPLAIN) {
      target = ((SqlExplain) target).getExplicandum();
    }
    if (target.getKind() == SqlKind.CREATE_TABLE) {
      target = ((SqlCreateTable) target).getQuery();
    }
    if (target instanceof SqlInsertDirectory) {
      target = ((SqlInsertDirectory) target).getQuery();
    }
    final SqlNode validated = planner.validate(target);
    final RelRoot root = planner.rel(validated);

    final List<RelDataType> parameterTypes = Lists.newArrayList();
    final RexShuttle collector = new RexShuttle()
    {
      @Override
      public RexNode visitDynamicParam(RexDynamicParam param)
      {
        final int index = param.getIndex();
        for (int i = parameterTypes.size(); i <= index; i++) {
          parameterTypes.add(null);
        }
        parameterTypes.set(index, param.getType());
        return param;
      }
    };
    final RelVisitor visitor = new RelVisitor()
    {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent)
      {
        super.visit(node, ordinal, parent);
        node.accept(collector);
      }
    };

    root.rel.accept(collector);
    root.rel.childrenAccept(visitor);
    return planWithDruidConvention(source, root, parameterTypes);
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  @Override
  public void close()
  {
    planner.close();
  }

  private PlannerResult planWithDruidConvention(
      final SqlNode source,
      final RelRoot root,
      final List<RelDataType> parameterTypes
  )
      throws RelConversionException
  {
    final DruidRel druidRel = (DruidRel) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        root.rel
    );

    if (source.getKind() == SqlKind.EXPLAIN) {
      return handleExplain(root.rel, druidRel, (SqlExplain) source);
    } else if (source.getKind() == SqlKind.CREATE_TABLE) {
      return handleCTAS(Utils.getFieldNames(root), druidRel, (SqlCreateTable) source);
    } else if (source instanceof SqlInsertDirectory) {
      return handleInsertDirectory(Utils.getFieldNames(root), druidRel, (SqlInsertDirectory) source);
    }

    final Function<List<Object>, Sequence<Object[]>> function = parameters ->
    {
      DruidQuery druidQuery = toDruidQuery(druidRel, parameterTypes, parameters);
      Query query = queryMaker.prepareQuery(druidQuery.getQuery());

      Execs.SettableFuture future = new Execs.SettableFuture<Object>();
      plannerContext.getQueryManager().register(query, future);
      Sequence<Object[]> sequence = queryMaker.runQuery(druidQuery, query);
      if (!root.isRefTrivial()) {
        // Add a mapping on top to accommodate root.fields.
        final int[] indices = Utils.getFieldIndices(root);
        sequence = Sequences.map(
            sequence,
            input -> {
              final Object[] retVal = new Object[root.fields.size()];
              for (int i = 0; i < indices.length; i++) {
                retVal[i] = input[indices[i]];
              }
              return retVal;
            }
        );
      }
      return Sequences.withBaggage(sequence, future);
    };
    final Set<String> datasourceNames = ImmutableSet.copyOf(druidRel.getDataSource().getNames());
    return new PlannerResult(function, root.validatedRowType, parameterTypes, datasourceNames);
  }

  private DruidQuery toDruidQuery(DruidRel druidRel, List<RelDataType> parameterTypes, List<Object> parameters)
  {
    if (parameterTypes.isEmpty() || parameters.isEmpty()) {
      return druidRel.toDruidQuery(false);
    }
    RelDataTypeFactory factory = druidRel.getCluster().getTypeFactory();
    RexBuilder builder = druidRel.getCluster().getRexBuilder();
    List<RexNode> literals = Lists.newArrayList();
    for(int i = 0; i < parameterTypes.size(); i++) {
      RelDataType type = parameterTypes.get(i);
      Object value = i < parameters.size() ? parameters.get(i) : null;
      if (value != null && type.isNullable()) {
        type = factory.createTypeWithNullability(type, false);
      }
      literals.add(builder.makeLiteral(value, type, true));
    }
    PlannerContext.PARAMETER_BINDING.set(new RexShuttle()
    {
      @Override
      public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
      {
        Preconditions.checkArgument(dynamicParam.getIndex() < parameters.size());
        return literals.get(dynamicParam.getIndex());
      }
    });
    try {
      return druidRel.toDruidQuery(false);
    }
    finally {
      PlannerContext.PARAMETER_BINDING.remove();
    }
  }

  private PlannerResult handleExplain(final RelNode source, final RelNode rel, final SqlExplain explain)
  {
    final String explanation;
    if (explain.withImplementation() && rel instanceof DruidRel) {
      try {
        plannerContext.disableQueryId();
        Query query = ((DruidRel) rel).toDruidQuery(true).getQuery();
        explanation = plannerContext.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(query);
      }
      catch (JsonProcessingException e) {
        throw QueryException.wrapIfNeeded(e);
      }
    } else if (explain.withType()) {
      explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    } else {
      explanation = RelOptUtil.dumpPlan("", source, explain.getFormat(), explain.getDetailLevel());
    }
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final RelDataType resultType = typeFactory.createStructType(
        ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        ImmutableList.of("PLAN")
    );
    return new PlannerResult(params -> Sequences.simple(ImmutableList.of(new Object[]{explanation})), resultType);
  }

  @SuppressWarnings("unchecked")
  private PlannerResult handleCTAS(
      final List<String> mappedColumns,
      final DruidRel druidRel,
      final SqlCreateTable source
  )
  {
    boolean temporary = source.isTemporary();
    SqlNode table = source.getTable();
    String dataSource;
    if (table instanceof SqlIdentifier) {
      dataSource = table.toString();
    } else if (table instanceof SqlCall && ((SqlCall) table).getOperator() == SqlStdOperatorTable.EXTEND) {
      List<SqlNode> operands = ((SqlCall) table).getOperandList();
      dataSource = operands.get(0).toString();
      SqlNodeList extend = (SqlNodeList) operands.get(1);
      if (extend != null) {
        // todo
        List<SqlNode> elements = extend.getList();
        for (int i = 0; i < elements.size(); i += 2) {
          String column = elements.get(i).toString();
          SqlIdentifier typeName = ((SqlDataTypeSpec) elements.get(i + 1)).getTypeNameSpec().getTypeName();
//          typeMap.put(column, Utils.TYPE_FACTORY.createType(typeName.toString()));
        }
      }
    } else {
      throw new IAE("unsupported type %s", table);
    }
    DruidQuery druidQuery = druidRel.toDruidQuery(false);

    RowSignature rowSignature = druidQuery.getOutputRowSignature();
    Map<String, String> mapping = ImmutableMap.of();
    if (source.getColumnList() != null) {
      List<String> columns = GuavaUtils.transform(source.getColumnList().getList(), Functions.toStringFunction());
      mapping = GuavaUtils.zipAsMap(rowSignature.getColumnNames(), columns);
    } else if (!Iterables.elementsEqual(rowSignature.getColumnNames(), mappedColumns)) {
      mapping = GuavaUtils.zipAsMap(rowSignature.getColumnNames(), mappedColumns);
    }
    IncrementalIndexSchema schema = IncrementalIndexSchema.from(rowSignature, mapping);

    Map<String, Object> forwardContext = GuavaUtils.mutableMap(
        FORMAT, INDEX_FORMAT,
        DATASOURCE, dataSource,
        REGISTER_TABLE, true,
        TEMPORARY, temporary,
        SCHEMA, schema
    );
    // extract timestamp column
    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      if (entry.getValue().equals(Column.TIME_COLUMN_NAME)) {
        forwardContext.put(TIMESTAMP_COLUMN, entry.getKey());
        break;
      }
    }
    Map<String, Object> context = ImmutableMap.of(
        Query.FORWARD_URL, LOCAL_TEMP_URL, Query.FORWARD_CONTEXT, forwardContext
    );
    Query<Map<String, Object>> query = queryMaker.prepareQuery(druidQuery.getQuery())
                                                 .withOverriddenContext(context);
    Map<String, Object> result = Sequences.only(QueryRunners.run(query, queryMaker.getSegmentWalker()), null);
    if (result == null) {
      return makeResult(Arrays.asList("success", "reason"), Arrays.asList(false, "empty"));
    }

    RelDataTypeFactory typeFactory = planner.getTypeFactory();
    RelDataType dataType = typeFactory.createStructType(
        Arrays.asList(
            typeFactory.createJavaType(boolean.class),
            typeFactory.createJavaType(int.class),
            typeFactory.createJavaType(String.class),
            typeFactory.createJavaType(int.class),
            typeFactory.createJavaType(String.class),
            typeFactory.createJavaType(String.class)
        ),
        Arrays.asList("success", "rowCount", "location", "length", "interval", "version")
    );
    List<Object[]> segments = Lists.newArrayList();
    for (Map<String, Object> data : (List<Map<String, Object>>) result.get("data")) {
      DataSegment segment = (DataSegment) data.get("segment");
      segments.add(new Object[]{
          true,
          segment.getNumRows(),
          data.get("location"),
          data.get("length"),
          String.valueOf(segment.getInterval()),
          segment.getVersion()
      });
    }
    return new PlannerResult(params -> Sequences.<Object[]>simple(segments), dataType);
  }

  private PlannerResult handleDescPath(SqlDescPath source)
  {
    final RelDataTypeFactory factory = planner.getTypeFactory();
    final RelDataType resultType = factory.createStructType(Arrays.asList(
        Pair.of("typeString", factory.createSqlType(SqlTypeName.VARCHAR)),
        Pair.of("extension", factory.createSqlType(SqlTypeName.VARCHAR)),
        Pair.of("inputFormat", factory.createSqlType(SqlTypeName.VARCHAR)),
        Pair.of("basePath", factory.createSqlType(SqlTypeName.VARCHAR)),
        Pair.of("paths", factory.createSqlType(SqlTypeName.ARRAY))
    ));
    FileLoadSpec loadSpec = resolve(source.asResolver(), queryMaker.getSegmentWalker());
    DataSchema dataSchema = loadSpec.getSchema();
    InputRowParser parser = dataSchema.getParser(plannerContext.getObjectMapper(), false);
    Object[] result = new Object[]{
        dataSchema.asTypeString(parser), loadSpec.getExtension(), loadSpec.getInputFormat(),
        loadSpec.getBasePath(), loadSpec.getPaths()
    };
    return new PlannerResult(params -> Sequences.<Object[]>of(result), resultType);
  }

  private PlannerResult handleLoadTable(SqlLoadTable source, BrokerServerView serverView)
  {
    final RelDataTypeFactory factory = planner.getTypeFactory();
    final RelDataType resultType = factory.createStructType(Arrays.asList(
        Pair.of("dataSource", factory.createSqlType(SqlTypeName.VARCHAR)),
        Pair.of("rowCount", factory.createSqlType(SqlTypeName.INTEGER)),
        Pair.of("indexedRowCount", factory.createSqlType(SqlTypeName.INTEGER)),
        Pair.of("indexedLength", factory.createSqlType(SqlTypeName.INTEGER)),
        Pair.of("numSegments", factory.createSqlType(SqlTypeName.INTEGER)),
        Pair.of("data", factory.createArrayType(factory.createSqlType(SqlTypeName.VARCHAR), -1))
    ));

    final FileLoadSpec resolved = resolve(source, queryMaker.getSegmentWalker());
    final LoadQuery query = LoadQuery.of(resolved);
    final Sequence<Object[]> sequence = Sequences.map(
        QueryRunners.run(query, queryMaker.getSegmentWalker()), Rows.mapToArray(resultType.getFieldNames())
    );

    return new PlannerResult(
        params -> sequence, resultType, null, ImmutableSet.of(resolved.getSchema().getDataSource())
    );
  }

  private FileLoadSpec resolve(SqlLoadTable source, QuerySegmentWalker segmentWalker)
  {
    String path = source.getPath();
    String dataSource = source.getTable().toString();
    Map<String, Object> properties = Maps.newHashMap(source.getProperties());
    properties.put("basePath", path);

    ObjectMapper mapper = plannerContext.getObjectMapper().copy();
    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    FileLoadSpec.Resolver resolver = mapper.convertValue(properties, FileLoadSpec.Resolver.class);
    if (resolver instanceof TypeStringResolver && properties.containsKey("format")) {
      // fucking jackson prohibits accessing type property
      ((TypeStringResolver) resolver).property("type", properties.get("format"));
    }
    try {
      return resolver.resolve(dataSource, segmentWalker)
                     .augment(
                         source.isTemporary(),
                         source.isOverwrite(),
                         properties
                     );
    }
    catch (IOException e) {
      throw new IAE(e, "Failed to resolve schema");
    }
  }

  @SuppressWarnings("unchecked")
  private PlannerResult handleInsertDirectory(
      final List<String> mappedColumns,
      final DruidRel druidRel,
      final SqlInsertDirectory source
  )
  {
    DruidQuery druidQuery = druidRel.toDruidQuery(false);

    Map<String, Object> context = Maps.newHashMap();
    Map forwardContext = Maps.newHashMap();
    if (source.getProperties() != null) {
      forwardContext.putAll(source.getProperties());
    }

    forwardContext.put(FORMAT, source.getFormat());
    forwardContext.put(CLEANUP, source.isOverwrite());

    RowSignature rowSignature = druidQuery.getOutputRowSignature();
    forwardContext.put(COLUMNS, rowSignature.getColumnNames());
    forwardContext.put(MAPPED_COLUMNS, mappedColumns);

    String typeString = rowSignature.replaceColumnNames(mappedColumns).asTypeString();

    forwardContext.put(TYPE_STRING, typeString);    // for orc

    context.put(Query.FORWARD_URL, source.getDirectory());
    context.put(Query.FORWARD_CONTEXT, forwardContext);

    Query<Map<String, Object>> query = queryMaker.prepareQuery(druidQuery.getQuery())
                                                 .withOverriddenContext(context);
    Map<String, Object> result = Sequences.only(QueryRunners.run(query, queryMaker.getSegmentWalker()), null);
    if (result == null) {
      return makeResult(Arrays.asList("success", "reason"), Arrays.asList(false, "empty"));
    }

    RelDataTypeFactory typeFactory = planner.getTypeFactory();
    RelDataType dataType = typeFactory.createStructType(
        Arrays.asList(
            typeFactory.createJavaType(boolean.class),
            typeFactory.createJavaType(int.class),
            typeFactory.createJavaType(String.class),
            typeFactory.createJavaType(int.class)
        ),
        Arrays.asList("success", "rowCount", "location", "length")
    );
    Map.Entry<String, Object> data = Iterables.getOnlyElement(
        ((Map<String, Object>) result.get("data")).entrySet()
    );
    Object[] row = new Object[]{true, result.get("rowCount"), data.getKey(), data.getValue()};
    return new PlannerResult(params -> Sequences.<Object[]>of(row), dataType);
  }

  private PlannerResult handleDropTable(final SqlDropTable source, final BrokerServerView brokerServerView)
  {
    if (!source.isTemporary()) {
      throw new UnsupportedOperationException("Not support dropping non-temporary table");
    }
    boolean success = brokerServerView.dropLocalDataSource(source.getName().toString());
    return makeResult(Arrays.asList("success"), Arrays.asList(success));
  }

  private PlannerResult makeResult(final List<String> names, final List values)
  {
    RelDataTypeFactory typeFactory = planner.getTypeFactory();
    List<RelDataType> relTypes = Lists.newArrayList();
    for (Object value : values) {
      relTypes.add(typeFactory.createJavaType(value == null ? String.class : value.getClass()));
    }
    RelDataType dataType = typeFactory.createStructType(relTypes, names);
    return new PlannerResult(params -> Sequences.<Object[]>of(values.toArray()), dataType);
  }
}
