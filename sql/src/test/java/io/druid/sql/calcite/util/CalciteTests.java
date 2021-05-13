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

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.client.DruidLeaderClient;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.data.ValueDesc;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.guice.GuiceAnnotationIntrospector;
import io.druid.guice.GuiceInjectableValues;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Processing;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.emitter.core.NoopEmitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.HttpClient;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.server.DruidNode;
import io.druid.server.QueryLifecycleFactory;
import io.druid.server.QueryManager;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Escalator;
import io.druid.server.security.NoopEscalator;
import io.druid.server.security.ResourceType;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.curator.x.discovery.ServiceProvider;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String TEST_SUPERUSER_NAME = "testSuperuser";

  public static final AuthorizerMapper TEST_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (authenticationResult.getIdentity().equals(TEST_SUPERUSER_NAME)) {
          return Access.OK;
        }

        if (resource.getType() == ResourceType.DATASOURCE && resource.getName().equals(FORBIDDEN_DATASOURCE)) {
          return new Access(false);
        } else {
          return Access.OK;
        }
      };
    }
  };
  public static final AuthenticatorMapper TEST_AUTHENTICATOR_MAPPER;

  static {
    final Map<String, Authenticator> defaultMap = new HashMap<>();
    defaultMap.put(
        AuthConfig.ALLOW_ALL_NAME,
        new AllowAllAuthenticator()
        {
          @Override
          public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
          {
            return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
          }
        }
    );
    TEST_AUTHENTICATOR_MAPPER = new AuthenticatorMapper(defaultMap);
  }

  public static final Escalator TEST_AUTHENTICATOR_ESCALATOR;

  static {
    TEST_AUTHENTICATOR_ESCALATOR = new NoopEscalator()
    {

      @Override
      public AuthenticationResult createEscalatedAuthenticationResult()
      {
        return SUPER_USER_AUTH_RESULT;
      }
    };
  }

  public static final AuthenticationResult REGULAR_USER_AUTH_RESULT = new AuthenticationResult(
      AuthConfig.ALLOW_ALL_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null, null
  );

  public static final AuthenticationResult SUPER_USER_AUTH_RESULT = new AuthenticationResult(
      TEST_SUPERUSER_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null, null
  );

  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "foo3";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";

  private static final String TIMESTAMP_COLUMN = "t";

  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static final Injector INJECTOR = Guice.createInjector(
      new Module()
      {
        @Override
        public void configure(final Binder binder)
        {
          binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.JSON_MAPPER);
          binder.bind(QueryToolChestWarehouse.class).toInstance(
              new QueryToolChestWarehouse()
              {
                @Override
                public QueryConfig getQueryConfig()
                {
                  return new QueryConfig();
                }

                @Override
                public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
                {
                  return TestHelper.CONGLOMERATE.findFactory(query).getToolchest();
                }
              }
          );
          binder.bind(Key.get(ExecutorService.class, Processing.class)).toInstance(
              Execs.newDirectExecutorService()
          );
        }
      }
  );

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new DefaultTimestampSpec(TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2")),
              null,
              null
          )
      )
  );

  private static final IncrementalIndexSchema INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new GenericSumAggregatorFactory("m1", "m1", ValueDesc.FLOAT),
          new GenericSumAggregatorFactory("m2", "m2", ValueDesc.DOUBLE),
          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
      )
      .withRollup(false)
      .build();

  public static final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(
          ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "m2", "1.0", "dim1", "", "dim2", ImmutableList.of("a"))
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "m2", "2.0", "dim1", "10.1", "dim2", ImmutableList.of())
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "m2", "3.0", "dim1", "2", "dim2", ImmutableList.of(""))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "m2", "4.0", "dim1", "1", "dim2", ImmutableList.of("a"))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "m2", "5.0", "dim1", "def", "dim2", ImmutableList.of("abc"))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-03", "m1", "6.0", "m2", "6.0", "dim1", "abc")
      )
  );

  public static final List<InputRow> ROWS2 = ImmutableList.of(
      createRow("2000-01-01", "דרואיד", "he", 1.0),
      createRow("2000-01-01", "druid", "en", 1.0),
      createRow("2000-01-01", "друид", "ru", 1.0)
  );

  public static final List<InputRow> ROWS3 = ImmutableList.of(
      createRow(
          ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "m2", "1.0", "dim1", "", "dim2", ImmutableList.of("a", "b"))
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "m2", "2.0", "dim1", "10.1", "dim2", ImmutableList.of())
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "m2", "3.0", "dim1", "2", "dim2", ImmutableList.of("b"))
      )
  );

  public static final List<InputRow> FORBIDDEN_ROWS = ImmutableList.of(
      createRow("2000-01-01", "forbidden", "abcd", 9999.0)
  );

  private CalciteTests()
  {
    // No instantiation.
  }

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(final QuerySegmentWalker walker)
  {
    return new QueryLifecycleFactory(
        new QueryManager(),
        new QueryToolChestWarehouse()
        {
          @Override
          public QueryConfig getQueryConfig()
          {
            return null;
          }

          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            final QueryRunnerFactory<T, QueryType> factory = TestHelper.CONGLOMERATE.findFactory(query);
            return factory == null ? null : factory.getToolchest();
          }
        },
        walker,
        new DefaultGenericQueryMetricsFactory(INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class))),
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        jsonMapper,
        TEST_AUTHORIZER_MAPPER
    );
  }

  public static SqlLifecycleFactory createSqlLifecycleFactory(final PlannerFactory plannerFactory)
  {
    return new SqlLifecycleFactory(
        plannerFactory,
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        null
    );
  }

  public static ObjectMapper getJsonMapper()
  {
    ObjectMapper mapper = INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setInjectableValues(new GuiceInjectableValues(INJECTOR));
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    return mapper;
  }

  public static TestQuerySegmentWalker createMockWalker(final File tmpDir)
  {
    final QueryableIndex index1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .indexMerger(TestHelper.getTestIndexMergerV9())
                                              .schema(INDEX_SCHEMA)
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .indexMerger(TestHelper.getTestIndexMergerV9())
                                              .schema(INDEX_SCHEMA)
                                              .rows(ROWS2)
                                              .buildMMappedIndex();

    final QueryableIndex index3 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "3"))
                                              .indexMerger(TestHelper.getTestIndexMergerV9())
                                              .schema(INDEX_SCHEMA)
                                              .rows(ROWS3)
                                              .buildMMappedIndex();

    final QueryableIndex forbiddenIndex = IndexBuilder.create()
                                                      .tmpDir(new File(tmpDir, "forbidden"))
                                                      .indexMerger(TestHelper.getTestIndexMergerV9())
                                                      .schema(INDEX_SCHEMA)
                                                      .rows(FORBIDDEN_ROWS)
                                                      .buildMMappedIndex();

    return TestIndex.segmentWalker.duplicate().add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(index1.getInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE3)
                   .interval(index3.getInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index3
    ).add(
        DataSegment.builder()
                   .dataSource(FORBIDDEN_DATASOURCE)
                   .interval(forbiddenIndex.getInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        forbiddenIndex
    );
  }

  public static DruidOperatorTable createOperatorTable()
  {
    Set<AggregatorFactory.SQLBundle> bundles = Sets.newHashSet(
        AggregatorFactory.bundleSQL(new RelayAggregatorFactory.TimeFirst("<name>", "<columnName>", null)),
        AggregatorFactory.bundleSQL(new RelayAggregatorFactory.TimeLast("<name>", "<columnName>", null)),
        AggregatorFactory.bundleSQL(new RelayAggregatorFactory.Min("<name>", "<columnName>", null)),
        AggregatorFactory.bundleSQL(new RelayAggregatorFactory.Max("<name>", "<columnName>", null))
    );
    Set<SqlOperatorConversion> extractionOperators = new HashSet<>();

    try {
      return new DruidOperatorTable(ImmutableSet.of(), bundles, extractionOperators, ImmutableSet.of());
    }
    catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  public static DruidSchema createMockSchema(
      final TestQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    return createMockSchema(walker, plannerConfig, new NoopViewManager());
  }

  public static DruidSchema createMockSchema(
      final TestQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final ViewManager viewManager
  )
  {
    final DruidSchema schema = new DruidSchema(
        walker,
        new TestServerInventoryView(walker.getSegments()),
        viewManager
    );

    return schema;
  }

  public static InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return PARSER.parse((Map<String, Object>) map);
  }

  public static InputRow createRow(final Object t, final String dim1, final String dim2, final double m1)
  {
    return PARSER.parse(
        ImmutableMap.<String, Object>of(
            "t", new DateTime(t, ISOChronology.getInstanceUTC()).getMillis(),
            "dim1", dim1,
            "dim2", dim2,
            "m1", m1
        )
    );
  }

  public static SystemSchema createMockSystemSchema(
      final DruidSchema druidSchema,
      final TestQuerySegmentWalker walker
  )
  {
    final DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        EasyMock.createMock(HttpClient.class),
        "nodetype",
        "/simple/leader",
        new ServerDiscoverySelector(EasyMock.createMock(ServiceProvider.class))
    )
    {
    };
    final CoordinatorClient coordinatorClient = new CoordinatorClient(
        EasyMock.createMock(DruidNode.class),
        EasyMock.createMock(HttpClient.class),
        getJsonMapper(),
        new ServerDiscoverySelector(EasyMock.createMock(ServiceProvider.class))
    )
    {
    };
    final IndexingServiceClient indexingServiceClient = new IndexingServiceClient(
        EasyMock.createMock(HttpClient.class),
        getJsonMapper(),
        new ServerDiscoverySelector(EasyMock.createMock(ServiceProvider.class))
    ){

    };
    final SystemSchema schema = new SystemSchema(
        druidSchema,
        new TestServerInventoryView(walker.getSegments()),
        coordinatorClient,
        indexingServiceClient,
        createOperatorTable(),
        getJsonMapper()
    );
    return schema;
  }
}
