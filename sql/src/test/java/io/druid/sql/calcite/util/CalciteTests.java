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

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.guice.GuiceAnnotationIntrospector;
import io.druid.guice.GuiceInjectableValues;
import io.druid.guice.annotations.Json;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.StreamQueryEngine;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.select.StreamRawQueryRunnerFactory;
import io.druid.query.select.StreamRawQueryToolChest;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";

  public static final String TEST_SUPERUSER_NAME = "testSuperuser";

  private static final String TIMESTAMP_COLUMN = "t";

  private static final Supplier<GroupByQueryConfig> GBY_CONF = new Supplier<GroupByQueryConfig>()
  {
    @Override
    public GroupByQueryConfig get()
    {
      return new GroupByQueryConfig();
    }
  };
  private static final Supplier<ByteBuffer> GBY_SUP = new Supplier<ByteBuffer>()
  {
    @Override
    public ByteBuffer get()
    {
      return ByteBuffer.allocate(10 * 1024 * 1024);
    }
  };
  private static final StupidPool<ByteBuffer> GBY_POOL = new StupidPool<ByteBuffer>(GBY_SUP);
  private static final GroupByQueryEngine GBY_ENGINE = new GroupByQueryEngine(
      GBY_CONF,
      new StupidPool<ByteBuffer>(
          new Supplier<ByteBuffer>()
          {
            @Override
            public ByteBuffer get()
            {
              return ByteBuffer.allocate(1024 * 1024);
            }
          }
      )
  );

  private static final Injector INJECTOR = Guice.createInjector(
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
                public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
                {
                  return CONGLOMERATE.findFactory(query).getToolchest();
                }
              }
          );
          // This Module is just to get a LookupReferencesManager with a usable "lookyloo" lookup.

//          binder.bind(LookupReferencesManager.class)
//                .toInstance(
//                    LookupEnabledTestExprMacroTable.createTestLookupReferencesManager(
//                        ImmutableMap.of(
//                            "a", "xa",
//                            "abc", "xabc"
//                        )
//                    )
//            );

        }
      }
  );

  private static final QueryRunnerFactoryConglomerate CONGLOMERATE = new DefaultQueryRunnerFactoryConglomerate(
      ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
          .put(
              SegmentMetadataQuery.class,
              new SegmentMetadataQueryRunnerFactory(
                  new SegmentMetadataQueryQueryToolChest(
                      new SegmentMetadataQueryConfig("P1W")
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              StreamRawQuery.class,
              new StreamRawQueryRunnerFactory(
                  new StreamRawQueryToolChest(),
                  new StreamQueryEngine()
              )
          )
          .put(
              SelectQuery.class,
              new SelectQueryRunnerFactory(
                  new SelectQueryQueryToolChest(
                      TestHelper.JSON_MAPPER,
                      new SelectQueryEngine(),
                      new SelectQueryConfig(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  new SelectQueryEngine(),
                  new SelectQueryConfig(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TimeseriesQuery.class,
              new TimeseriesQueryRunnerFactory(
                  new TimeseriesQueryQueryToolChest(
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  new TimeseriesQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TopNQuery.class,
              new TopNQueryRunnerFactory(
                  new StupidPool<>(
                      new Supplier<ByteBuffer>()
                      {
                        @Override
                        public ByteBuffer get()
                        {
                          return ByteBuffer.allocate(10 * 1024 * 1024);
                        }
                      }
                  ),
                  new TopNQueryQueryToolChest(
                      new TopNQueryConfig(),
                      new TopNQueryEngine(new StupidPool<>(
                      new Supplier<ByteBuffer>()
                      {
                        @Override
                        public ByteBuffer get()
                        {
                          return ByteBuffer.allocate(10 * 1024 * 1024);
                        }
                      }
                  )),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              GroupByQuery.class,
              new GroupByQueryRunnerFactory(
                  GBY_ENGINE,
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER,
                  GBY_CONF,
                  new GroupByQueryQueryToolChest(
                      GBY_CONF,
                      GBY_ENGINE,
                      GBY_POOL,
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  GBY_POOL
              )
          )
          .build()
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
          new GenericSumAggregatorFactory("m1", "m1", "float"),
          new GenericSumAggregatorFactory("m2", "m2", "double"),
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

  public static final List<InputRow> FORBIDDEN_ROWS = ImmutableList.of(
      createRow("2000-01-01", "forbidden", "abcd", 9999.0)
  );

  private CalciteTests()
  {
    // No instantiation.
  }

  public static QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate()
  {
    return CONGLOMERATE;
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

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(final File tmpDir)
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

    final QueryableIndex forbiddenIndex = IndexBuilder.create()
                                                      .tmpDir(new File(tmpDir, "forbidden"))
                                                      .indexMerger(TestHelper.getTestIndexMergerV9())
                                                      .schema(INDEX_SCHEMA)
                                                      .rows(FORBIDDEN_ROWS)
                                                      .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(queryRunnerFactoryConglomerate()).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(index1.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(FORBIDDEN_DATASOURCE)
                   .interval(forbiddenIndex.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        forbiddenIndex
    );
  }

  public static DruidOperatorTable createOperatorTable()
  {
    try {
      final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();
//      extractionOperators.add(INJECTOR.getInstance(LookupOperatorConversion.class));
      return new DruidOperatorTable(ImmutableSet.of(), extractionOperators);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static DruidSchema createMockSchema(
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    return createMockSchema(walker, plannerConfig, new NoopViewManager());
  }

  public static DruidSchema createMockSchema(
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final ViewManager viewManager
  )
  {
    final DruidSchema schema = new DruidSchema(
        walker,
        new TestServerInventoryView(walker.getSegments()),
        plannerConfig,
        viewManager
    );

    schema.start();
    try {
      schema.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    schema.stop();
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
}