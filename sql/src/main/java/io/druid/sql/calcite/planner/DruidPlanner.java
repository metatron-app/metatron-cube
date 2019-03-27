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

package io.druid.sql.calcite.planner;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.client.BrokerServerView;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.server.ForwardConstants;
import io.druid.sql.calcite.ddl.SqlCreateTable;
import io.druid.sql.calcite.ddl.SqlDropTable;
import io.druid.sql.calcite.rel.DruidConvention;
import io.druid.sql.calcite.rel.DruidQuery;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.timeline.DataSegment;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DruidPlanner implements Closeable, ForwardConstants
{
  private final Planner planner;
  private final PlannerContext plannerContext;

  public DruidPlanner(Planner planner, PlannerContext plannerContext)
  {
    this.planner = planner;
    this.plannerContext = plannerContext;
  }

  public PlannerResult plan(
      final String sql,
      final BrokerServerView brokerServerView,
      final HttpServletRequest request
  )
      throws SqlParseException, ValidationException, RelConversionException
  {
    final SqlNode source = planner.parse(sql);
    if (source.getKind() == SqlKind.DROP_TABLE) {
      return handleDropTable((SqlDropTable) source, brokerServerView);
    }
    SqlNode target = source;
    if (target.getKind() == SqlKind.EXPLAIN) {
      target = ((SqlExplain) target).getExplicandum();
    }
    if (target.getKind() == SqlKind.CREATE_TABLE) {
      target = ((SqlCreateTable) target).getQuery();
    }
    final SqlNode validated = planner.validate(target);
    final RelRoot root = planner.rel(validated);

    try {
      return planWithDruidConvention(source, root, brokerServerView, request);
    }
    catch (RelOptPlanner.CannotPlanException e) {
      // Try again with BINDABLE convention. Used for querying Values, metadata tables, and fallback.
      try {
        if (source.getKind() != SqlKind.CREATE_TABLE) {
          return planWithBindableConvention(source, root, request);
        }
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
        throw e;
      }
      throw e;
    }
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
      final BrokerServerView brokerServerView,
      final HttpServletRequest request
  ) throws RelConversionException
  {
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        root.rel
    );

    if (source.getKind() == SqlKind.EXPLAIN) {
      return handleExplain(druidRel, (SqlExplain) source);
    } else if (source.getKind() == SqlKind.CREATE_TABLE) {
      return handleCTAS(druidRel, (SqlCreateTable) source, brokerServerView);
    }

    final QueryMaker queryMaker = druidRel.getQueryMaker();
    final DruidQuery druidQuery = druidRel.toDruidQuery(false);
    final Supplier<Sequence<Object[]>> resultsSupplier = new Supplier<Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> get()
      {
        Sequence<Object[]> sequence = queryMaker.runQuery(druidQuery);
        if (root.isRefTrivial()) {
          return sequence;
        }
        // Add a mapping on top to accommodate root.fields.
        return Sequences.map(
            sequence,
            new Function<Object[], Object[]>()
            {
              @Override
              public Object[] apply(final Object[] input)
              {
                final Object[] retVal = new Object[root.fields.size()];
                for (int i = 0; i < root.fields.size(); i++) {
                  retVal[i] = input[root.fields.get(i).getKey()];
                }
                return retVal;
              }
            }
        );
      }
    };
    return new PlannerResult(resultsSupplier, root.validatedRowType);
  }

  private PlannerResult planWithBindableConvention(
      final SqlNode source,
      final RelRoot root,
      final HttpServletRequest request
  ) throws RelConversionException
  {
    BindableRel bindableRel = (BindableRel) planner.transform(
        Rules.BINDABLE_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(BindableConvention.INSTANCE)
               .plus(root.collation),
        root.rel
    );

    if (!root.isRefTrivial()) {
      // Add a projection on top to accommodate root.fields.
      final List<RexNode> projects = new ArrayList<>();
      final RexBuilder rexBuilder = bindableRel.getCluster().getRexBuilder();
      for (int field : Pair.left(root.fields)) {
        projects.add(rexBuilder.makeInputRef(bindableRel, field));
      }
      bindableRel = new Bindables.BindableProject(
          bindableRel.getCluster(),
          bindableRel.getTraitSet(),
          bindableRel,
          projects,
          root.validatedRowType
      );
    }

    if (source.getKind() == SqlKind.EXPLAIN) {
      return handleExplain(bindableRel, (SqlExplain) source);
    } else {
      final BindableRel theRel = bindableRel;
      final DataContext dataContext = plannerContext.createDataContext((JavaTypeFactory) planner.getTypeFactory());
      final Supplier<Sequence<Object[]>> resultsSupplier = () -> {
        final Enumerable enumerable = theRel.bind(dataContext);
        final Enumerator enumerator = enumerable.enumerator();
        return Sequences.withBaggage(new BaseSequence<>(
            new BaseSequence.IteratorMaker<Object[], EnumeratorIterator<Object[]>>()
            {
              @Override
              public EnumeratorIterator<Object[]> make()
              {
                return new EnumeratorIterator<Object[]>(new Iterator<Object[]>()
                {
                  @Override
                  public boolean hasNext()
                  {
                    return enumerator.moveNext();
                  }

                  @Override
                  public Object[] next()
                  {
                    return (Object[]) enumerator.current();
                  }
                });
              }

              @Override
              public void cleanup(EnumeratorIterator iterFromMake) {}
            }
        ), enumerator::close);
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  private static class EnumeratorIterator<T> implements Iterator<T>
  {
    private final Iterator<T> it;

    public EnumeratorIterator(Iterator<T> it)
    {
      this.it = it;
    }

    @Override
    public boolean hasNext()
    {
      return it.hasNext();
    }

    @Override
    public T next()
    {
      return it.next();
    }
  }

  private PlannerResult handleExplain(final RelNode rel, final SqlExplain explain)
  {
    final String explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    final Supplier<Sequence<Object[]>> resultsSupplier = Suppliers.ofInstance(
        Sequences.simple(ImmutableList.of(new Object[]{explanation})));
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    return new PlannerResult(
        resultsSupplier,
        typeFactory.createStructType(
            ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            ImmutableList.of("PLAN")
        )
    );
  }

  @SuppressWarnings("unchecked")
  private PlannerResult handleCTAS(
      final DruidRel<?> druidRel,
      final SqlCreateTable source,
      final BrokerServerView brokerServerView
  )
  {
    boolean temporary = source.isTemporary();
    String dataSource = source.getName().toString();

    QueryMaker queryMaker = druidRel.getQueryMaker();
    DruidQuery druidQuery = druidRel.toDruidQuery(false);

    RowSignature rowSignature = druidQuery.getOutputRowSignature();
    IncrementalIndexSchema schema = rowSignature.asSchema().asRelaySchema();

    Map<String, Object> context = Maps.newHashMap();
    context.put(Query.FORWARD_URL, LOCAL_TEMP_URL);
    context.put(Query.FORWARD_CONTEXT, GuavaUtils.mutableMap(
        FORMAT, INDEX_FORMAT,
        DATASOURCE, dataSource,
        REGISTER_TABLE, true,
        TEMPORARY, temporary,
        SCHEMA, schema
    ));
    Query<Map<String, Object>> query = queryMaker.prepareQuery(
        druidQuery.getQuery().withOverriddenContext(context)
    );
    if (source.ifNotExists() && !brokerServerView.addLocalDataSource(dataSource)) {
      // warn : not synchronized properly
      return makeResult(Arrays.asList("success", "reason"), Arrays.asList(false, "already exists"));
    }
    Map<String, Object> result = Sequences.only(query.run(queryMaker.getSegmentWalker(), Maps.newHashMap()));

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
    Map<String, Object> data = (Map<String, Object>) result.get("data");
    DataSegment segment = (DataSegment) data.get("segment");
    Object[] row = new Object[]{
        true,
        result.get("rowCount"),
        data.get("location"),
        data.get("length"),
        segment.getInterval(),
        segment.getVersion()
    };
    return new PlannerResult(Suppliers.ofInstance(Sequences.<Object[]>of(row)), dataType);
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
    return new PlannerResult(Suppliers.ofInstance(Sequences.<Object[]>of(values.toArray())), dataType);
  }
}
