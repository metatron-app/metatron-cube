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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.QueryManager;
import io.druid.server.QueryLifecycleFactory;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;
import java.util.Properties;

public class PlannerFactory
{
  private static final SqlParser.Config PARSER_CONFIG = SqlParser
      .configBuilder()
      .setCaseSensitive(true)
      .setUnquotedCasing(Casing.UNCHANGED)
      .setQuotedCasing(Casing.UNCHANGED)
      .setQuoting(Quoting.DOUBLE_QUOTE)
      .setConformance(DruidConformance.instance())
      .setParserFactory(SqlParserImpl::new)
      .build();

  private final DruidSchema druidSchema;
  private final SystemSchema systemSchema;
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final QuerySegmentWalker segmentWalker;
  private final QueryManager queryManager;
  private final DruidOperatorTable operatorTable;
  private final PlannerConfig plannerConfig;
  private final QueryConfig queryConfig;
  private final ObjectMapper jsonMapper;

  @Inject
  public PlannerFactory(
      final DruidSchema druidSchema,
      final SystemSchema systemSchema,
      final QueryLifecycleFactory queryLifecycleFactory,
      final QuerySegmentWalker segmentWalker,
      final QueryManager queryManager,
      final DruidOperatorTable operatorTable,
      final PlannerConfig plannerConfig,
      final QueryConfig queryConfig,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.druidSchema = druidSchema;
    this.systemSchema = systemSchema;
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.segmentWalker = segmentWalker;
    this.queryManager = queryManager;
    this.operatorTable = operatorTable;
    this.plannerConfig = plannerConfig;
    this.queryConfig = queryConfig;
    this.jsonMapper = jsonMapper;
  }

  public DruidPlanner createPlanner(final Map<String, Object> queryContext)
  {
    final SchemaPlus rootSchema = Calcites.createRootSchema(druidSchema, segmentWalker, systemSchema);
    final PlannerContext plannerContext = PlannerContext.create(
        queryManager,
        operatorTable,
        plannerConfig,
        queryContext
    );
    final QueryMaker queryMaker = new QueryMaker(queryLifecycleFactory, segmentWalker, plannerContext, queryConfig, jsonMapper);
    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
        .configBuilder()
        .withExpand(false)
        .withDecorrelationEnabled(true)
        .withTrimUnusedFields(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .build();
    final FrameworkConfig frameworkConfig = Frameworks
        .newConfigBuilder()
        .parserConfig(PARSER_CONFIG)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(Rules.programs(plannerContext, queryMaker))
        .executor(new DruidRexExecutor(plannerContext))
        .context(Contexts.EMPTY_CONTEXT)
        .typeSystem(DruidTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(DruidSchema.NAME))
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .context(new Context()
        {
          @Override
          @SuppressWarnings("unchecked")
          public <C> C unwrap(final Class<C> aClass)
          {
            if (aClass.equals(CalciteConnectionConfig.class)) {
              // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
              // validator will not respect it.
              final Properties props = new Properties();
              return (C) new CalciteConnectionConfigImpl(props)
              {
                @Override
                public SqlConformance conformance()
                {
                  return DruidConformance.instance();
                }
              };
            } else {
              return null;
            }
          }
        })
        .build();

    return new DruidPlanner(
        Frameworks.getPlanner(frameworkConfig),
        plannerContext
    );
  }
}
