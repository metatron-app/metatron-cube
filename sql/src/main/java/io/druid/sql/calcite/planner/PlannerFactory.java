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
import io.druid.query.QuerySegmentWalker;
import io.druid.server.QueryLifecycleFactory;
import io.druid.server.QueryManager;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.AuthorizerMapper;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.DruidCost;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Context;
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
      .config()
      .withCaseSensitive(true)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withQuoting(Quoting.DOUBLE_QUOTE)
      .withConformance(DruidConformance.instance())
      .withParserFactory(SqlParserImpl::new);

  private final DruidSchema druidSchema;
  private final SystemSchema systemSchema;
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final QuerySegmentWalker segmentWalker;
  private final QueryManager queryManager;
  private final DruidOperatorTable operatorTable;
  private final AuthorizerMapper authorizerMapper;
  private final PlannerConfig plannerConfig;
  private final ObjectMapper jsonMapper;

  @Inject
  public PlannerFactory(
      final DruidSchema druidSchema,
      final SystemSchema systemSchema,
      final QueryLifecycleFactory queryLifecycleFactory,
      final QuerySegmentWalker segmentWalker,
      final QueryManager queryManager,
      final DruidOperatorTable operatorTable,
      final AuthorizerMapper authorizerMapper,
      final PlannerConfig plannerConfig,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.druidSchema = druidSchema;
    this.systemSchema = systemSchema;
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.segmentWalker = segmentWalker;
    this.queryManager = queryManager;
    this.operatorTable = operatorTable;
    this.authorizerMapper = authorizerMapper;
    this.plannerConfig = plannerConfig;
    this.jsonMapper = jsonMapper;
  }

  public PlannerContext createContext()
  {
    return createContext(null, null);
  }

  public PlannerContext createContext(Map<String, Object> queryContext, AuthenticationResult authenticationResult)
  {
    return PlannerContext.create(
        queryManager,
        operatorTable,
        plannerConfig,
        jsonMapper,
        queryContext,
        authenticationResult
    );
  }

  public DruidPlanner createPlanner(Map<String, Object> queryContext, AuthenticationResult authenticationResult)
  {
    final PlannerContext plannerContext = createContext(queryContext, authenticationResult);
    final SchemaPlus rootSchema = Calcites.createRootSchema(druidSchema, segmentWalker, systemSchema);
    final QueryMaker queryMaker = new QueryMaker(queryLifecycleFactory, segmentWalker, plannerContext);
    final FrameworkConfig frameworkConfig = Frameworks
        .newConfigBuilder()
        .executor(new DruidRexExecutor(plannerContext))
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .parserConfig(PARSER_CONFIG)
        .sqlToRelConverterConfig(
            SqlToRelConverter.config()
                             .withExpand(false)
                             .withDecorrelationEnabled(true)
                             .withTrimUnusedFields(false)
                             .withInSubQueryThreshold(Integer.MAX_VALUE))
        .defaultSchema(rootSchema.getSubSchema(DruidSchema.NAME))
        .costFactory(DruidCost.FACTORY)
        .programs(Rules.programs(queryMaker))
        .typeSystem(DruidTypeSystem.INSTANCE)
        .context(new Context()
        {
          @Override
          @SuppressWarnings("unchecked")
          public <C> C unwrap(final Class<C> aClass)
          {
            // stupid detour of calcite way (see PlannerImpl.connConfig)
            if (CalciteConnectionConfig.class.isAssignableFrom(aClass)) {
              Properties props = new Properties();
              props.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), true);
              props.put(CalciteConnectionProperty.CONFORMANCE.camelName(), DruidConformance.instance());
              return (C) new CalciteConnectionConfigImpl(props)
              {
                @Override
                public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem)
                {
                  return (T) DruidTypeSystem.INSTANCE;
                }

                @Override
                public SqlConformance conformance()
                {
                  return DruidConformance.instance();
                }
              };
            }
            return null;
          }
        })
        .build();

    return new DruidPlanner(
        Frameworks.getPlanner(frameworkConfig), plannerContext, queryMaker
    );
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }
}
