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

package io.druid.segment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.UTF8Bytes;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.ObjectMappers;
import io.druid.query.BySegmentResultValue;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.DefaultQueryMetrics;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.DimensionSamplingQuery;
import io.druid.query.DimensionSamplingQueryRunnerFactory;
import io.druid.query.DimensionSamplingQueryToolChest;
import io.druid.query.FilterMetaQuery;
import io.druid.query.FilterMetaQueryRunnerFactory;
import io.druid.query.FilterMetaQueryToolChest;
import io.druid.query.JoinQueryConfig;
import io.druid.query.NoopQueryWatcher;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.SchemaQuery;
import io.druid.query.SchemaQueryRunnerFactory;
import io.druid.query.SchemaQueryToolChest;
import io.druid.query.frequency.FrequencyQuery;
import io.druid.query.frequency.FrequencyQueryRunnerFactory;
import io.druid.query.frequency.FrequencyQueryToolChest;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.kmeans.FindNearestQuery;
import io.druid.query.kmeans.FindNearestQueryRunnerFactory;
import io.druid.query.kmeans.FindNearestQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaQueryEngine;
import io.druid.query.select.SelectMetaQueryRunnerFactory;
import io.druid.query.select.SelectMetaQueryToolChest;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryEngine;
import io.druid.query.select.StreamQueryRunnerFactory;
import io.druid.query.select.StreamQueryToolChest;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 */
public class TestHelper
{
  private static final IndexMerger INDEX_MERGER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals(QuerySegmentWalker.class.getName())) {
              return TestIndex.segmentWalker;
            } else if (valueId.equals(ExecutorService.class.getName())) {
              return TestIndex.segmentWalker.getExecutor();
            } else if (valueId.equals(QueryToolChestWarehouse.class.getName())) {
              return TestIndex.segmentWalker;
            } else if (valueId.equals(JoinQueryConfig.class.getName())) {
              return TestIndex.segmentWalker.getQueryConfig().getJoin();
            }
            return null;
          }
        }
    );
    INDEX_IO = new IndexIO(
        JSON_MAPPER
    );
    INDEX_MERGER = new IndexMerger(JSON_MAPPER, INDEX_IO);
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  public static final QueryWatcher NOOP_QUERYWATCHER = NoopQueryWatcher.instance();

  private static final StupidPool<ByteBuffer> GBY_POOL = StupidPool.heap(10 * 1024 * 1024);
  private static final GroupByQueryEngine GBY_ENGINE = new GroupByQueryEngine(StupidPool.heap(1024 * 1024));

  public static final QueryRunnerFactoryConglomerate CONGLOMERATE = newConglometator();

  public static QueryRunnerFactoryConglomerate newConglometator()
  {
    final QueryConfig config = new QueryConfig();
    return new DefaultQueryRunnerFactoryConglomerate(
        config,
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(
                TimeBoundaryQuery.class,
                new TimeBoundaryQueryRunnerFactory(NOOP_QUERYWATCHER)
            )
            .put(
                SegmentMetadataQuery.class,
                new SegmentMetadataQueryRunnerFactory(
                    new SegmentMetadataQueryQueryToolChest(
                        config.getSegmentMeta()
                    ),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                StreamQuery.class,
                new StreamQueryRunnerFactory(
                    new StreamQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    new StreamQueryEngine(),
                    config,
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                SelectQuery.class,
                new SelectQueryRunnerFactory(
                    new SelectQueryQueryToolChest(
                        new SelectQueryEngine(),
                        DefaultGenericQueryMetricsFactory.instance()
                    ),
                    new SelectQueryEngine(),
                    config.getSelect(),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(),
                    new TimeseriesQueryEngine(),
                    config,
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                TopNQuery.class,
                new TopNQueryRunnerFactory(
                    StupidPool.heap(10 * 1024 * 1024),
                    new TopNQueryQueryToolChest(
                        config.getTopN(),
                        new TopNQueryEngine(StupidPool.heap(10 * 1024 * 1024))
                    ),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                GroupByQuery.class,
                new GroupByQueryRunnerFactory(
                    GBY_ENGINE,
                    new StreamQueryEngine(),
                    NOOP_QUERYWATCHER,
                    config,
                    new GroupByQueryQueryToolChest(config, GBY_ENGINE, GBY_POOL),
                    GBY_POOL
                )
            )
            .put(
                SelectMetaQuery.class,
                new SelectMetaQueryRunnerFactory(
                    new SelectMetaQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    new SelectMetaQueryEngine(),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                FilterMetaQuery.class,
                new FilterMetaQueryRunnerFactory(
                    new FilterMetaQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                DimensionSamplingQuery.class,
                new DimensionSamplingQueryRunnerFactory(
                    new DimensionSamplingQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                SchemaQuery.class,
                new SchemaQueryRunnerFactory(
                    new SchemaQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                SearchQuery.class,
                new SearchQueryRunnerFactory(
                    new SearchQueryQueryToolChest(
                        new SearchQueryConfig()
                    ),
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                FindNearestQuery.class,
                new FindNearestQueryRunnerFactory(
                    new FindNearestQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    new StreamQueryEngine(),
                    config,
                    NOOP_QUERYWATCHER
                )
            )
            .put(
                FrequencyQuery.class,
                new FrequencyQueryRunnerFactory(
                    new FrequencyQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                    NOOP_QUERYWATCHER
                )
            )
            .build()
    );
  }

  public static <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> factoryFor(Class clazz)
  {
    return CONGLOMERATE.findFactory(clazz);
  }

  public static final TestQuerySegmentWalker salesWalker = newWalker();
  public static final TestQuerySegmentWalker profileWalker = newWalker();

  static {
    salesWalker.addSalesIndex().addCategoryAliasIndex();
    profileWalker.addProfileIndex();
  }

  public static TestQuerySegmentWalker newWalker()
  {
    return new TestQuerySegmentWalker(newConglometator());
  }

  public static class TestQueryRunnerFactory implements QueryRunnerFactory
  {
    @Override
    public Supplier<Object> preFactoring(Query query, List list, Supplier resolver, ExecutorService exec)
    {
      return null;
    }

    @Override
    public QueryRunner _createRunner(Segment segment, Supplier optimizer)
    {
      return null;
    }

    @Override
    public QueryRunner mergeRunners(Query query, ExecutorService queryExecutor, Iterable iterable, Supplier optimizer)
    {
      return null;
    }

    @Override
    public QueryToolChest getToolchest()
    {
      return new QueryToolChest()
      {
        @Override
        public QueryRunner mergeResults(QueryRunner runner)
        {
          return runner;
        }

        @Override
        public QueryMetrics makeMetrics(Query query)
        {
          return new DefaultQueryMetrics<>(new DefaultObjectMapper());
        }

        @Override
        public TypeReference getResultTypeReference(Query query)
        {
          return new TypeReference() {};
        }
      };
    }
  }

  public static ObjectMapper getTestObjectMapper()
  {
    return JSON_MAPPER;
  }


  public static IndexMerger getTestIndexMerger()
  {
    return INDEX_MERGER;
  }

  public static IndexMergerV9 getTestIndexMergerV9()
  {
    return INDEX_MERGER_V9;
  }

  public static IndexIO getTestIndexIO()
  {
    return INDEX_IO;
  }

  public static ObjectMapper getObjectMapper()
  {
    return JSON_MAPPER;
  }

  public static String printObjectPretty(Object object)
  {
    try {
      return JSON_MAPPER.writer(new DefaultPrettyPrinter()).writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ObjectMapper makeJsonMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class.getName(), mapper)
    );
    return mapper;
  }

  public static <T> List<T> runQuery(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    return Sequences.toList(query.run(segmentWalker, Maps.newHashMap()));
  }

  public static <T> Iterable<T> revert(Iterable<T> input)
  {
    return Lists.reverse(Lists.newArrayList(input));
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Sequence<Result<T>> results)
  {
    assertResults(expectedResults, Sequences.toList(results), "");
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    assertResults(expectedResults, results, "");
  }

  public static <T> void assertExpectedResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> results,
      String failMsg
  )
  {
    assertResults(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Iterable<T> results)
  {
    assertObjects(expectedResults, results, "");
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Iterable<T> results, String failMsg)
  {
    assertObjects(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Sequence<T> results)
  {
    assertExpectedObjects(expectedResults, results, "");
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Sequence<T> results, String failMsg)
  {
    assertObjects(expectedResults, Sequences.toList(results), failMsg);
  }

  private static <T> void assertResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> actualResults,
      String failMsg
  )
  {
    Iterator<? extends Result> resultsIter = actualResults.iterator();
    Iterator<? extends Result> expectedResultsIter = expectedResults.iterator();

    while (resultsIter.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        Assert.assertEquals(failMsg, expectedNext, next);
      } else {
        assertResult(failMsg, (Result) expectedNext, (Result) next);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              failMsg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static <T> void assertObjects(Iterable<T> expectedResults, Iterable<T> actualResults, String msg)
  {
    Iterator resultsIter = actualResults.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    int index = 0;
    while (resultsIter.hasNext() && expectedResultsIter.hasNext()) {
      final Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();

      String failMsg = msg + "-" + index++;

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        assertRow(failMsg, (Row) expectedNext, (Row) next);
      } else if (expectedNext instanceof Result && ((Result) expectedNext).getValue() instanceof BySegmentResultValue) {
        BySegmentResultValue ev = (BySegmentResultValue) ((Result) expectedNext).getValue();
        BySegmentResultValue rv = (BySegmentResultValue) ((Result) next).getValue();
        Assert.assertEquals(ev.getInterval(), rv.getInterval());
        Assert.assertEquals(ev.getSegmentId(), rv.getSegmentId());
        assertObjects(ev.getResults(), rv.getResults(), msg);
      } else {
        Assert.assertEquals(failMsg, expectedNext, next);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              msg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static void assertRow(String msg, Row expected, Row actual)
  {
    // Custom equals check to get fuzzy comparison of numerics, useful because different groupBy strategies don't
    // always generate exactly the same results (different merge ordering / float vs double)
    if (expected.getTimestamp() != null) {
      Assert.assertEquals(String.format("%s: timestamp", msg), expected.getTimestamp(), actual.getTimestamp());
    }

    final Map<String, Object> expectedMap = ((MapBasedRow) expected).getEvent();
    final Map<String, Object> actualMap = ((MapBasedRow) actual).getEvent();

//    Assert.assertEquals(String.format("%s: map keys", msg), expectedMap.keySet(), actualMap.keySet());
    for (final String key : expectedMap.keySet()) {
      final Object ev = expectedMap.get(key);
      final Object rv = actualMap.get(key);

      if (ev instanceof Float && rv instanceof Double || ev instanceof Double && rv instanceof Float) {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            ((Number) ev).doubleValue(),
            ((Number) rv).doubleValue(),
            ((Number) ev).doubleValue() * 1e-6
        );
      } else if (ev instanceof Long && rv instanceof Integer || ev instanceof Integer && rv instanceof Long) {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            ((Number) ev).longValue(),
            ((Number) rv).longValue()
        );
      } else if (ev instanceof String && rv instanceof UTF8Bytes || ev instanceof UTF8Bytes && rv instanceof String) {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            Objects.toString(ev, null),
            Objects.toString(rv, null)
        );
      } else if (ev instanceof Double[]) {
        Assert.assertArrayEquals(
            String.format("%s: key[%s]", msg, key),
            Doubles.toArray(Arrays.asList((Double[]) ev)),
            Doubles.toArray(Arrays.asList((Double[]) rv)),
            0.0001
        );
      } else if (ev != null && ev.getClass().isArray()) {
        int length = Array.getLength(ev);
        for (int i = 0; i < length; i++) {
          Assert.assertEquals(
              String.format("%s: key[%s.%d]", msg, key, i),
              Array.get(ev, i),
              Array.get(rv, i)
          );
        }
      } else {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            ev,
            rv
        );
      }
    }
  }

  private static void assertResult(String msg, Result<?> expected, Result actual)
  {
    Assert.assertEquals(msg, expected.getTimestamp(), actual.getTimestamp());
    Object o1 = expected.getValue();
    Object o2 = actual.getValue();
    Assert.assertEquals(msg, expected.getValue(), actual.getValue());
  }

  public static TopNQueryEngine testTopNQueryEngine()
  {
    return new TopNQueryEngine(StupidPool.heap(1024 * 1024));
  }

  public static List<InputRow> createInputRows(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    List<InputRow> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        }
      }
      DateTime timestamp = timeIndex < 0 ? new DateTime(0) : new DateTime(value[timeIndex]);
      expected.add(new MapBasedInputRow(timestamp, Arrays.asList(columnNames), theVals));
    }
    return expected;
  }

  public static QueryableIndex persistRealtimeAndLoadMMapped(IncrementalIndex index)
  {
    return persistRealtimeAndLoadMMapped(index, IndexSpec.DEFAULT, INDEX_IO);
  }

  public static QueryableIndex persistRealtimeAndLoadMMapped(IncrementalIndex index, IndexSpec indexSpec)
  {
    return persistRealtimeAndLoadMMapped(index, indexSpec, INDEX_IO);
  }

  public static QueryableIndex persistRealtimeAndLoadMMapped(IncrementalIndex index, IndexSpec indexSpec, IndexIO indexIO)
  {
    try {
      File someTmpFile = GuavaUtils.createTemporaryDirectory("billy", "yay");
      someTmpFile.deleteOnExit();

      indexIO.getIndexMerger().persist(index, someTmpFile, indexSpec);
      return indexIO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static void printJson(Object object)
  {
    ObjectWriter writer = ObjectMappers.excludeNulls(JSON_MAPPER)
                                       .writer(new DefaultPrettyPrinter());
    try {
      System.out.println(writer.writeValueAsString(object));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String[] array(String... objects)
  {
    return objects;
  }

  public static Object[] array(Object... objects)
  {
    return objects;
  }

  public static List list(Object... objects)
  {
    return Arrays.asList(objects);
  }

  public static List<Map<String, Object>> createExpectedMaps(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    List<Map<String, Object>> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        } else {
          theVals.put(columnNames[i], new DateTime(value[i]));
        }
      }
      expected.add(theVals);
    }
    return expected;
  }

  public static Map<String, Object> of(Object... keyvalues)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < keyvalues.length; i += 2) {
      builder.put(String.valueOf(keyvalues[i]), keyvalues[i + 1]);
    }
    return builder.build();
  }

  public static class RowBuilder
  {
    private final String[] names;
    private final List<Row> rows = Lists.newArrayList();

    public RowBuilder(String[] names)
    {
      this.names = names;
    }

    public RowBuilder add(final String timestamp, Object... values)
    {
      rows.add(build(timestamp, values));
      return this;
    }

    public List<Row> build()
    {
      try {
        return Lists.newArrayList(rows);
      }
      finally {
        rows.clear();
      }
    }

    public Row build(final String timestamp, Object... values)
    {
      Preconditions.checkArgument(names.length == values.length);

      Map<String, Object> theVals = Maps.newHashMap();
      for (int i = 0; i < values.length; i++) {
        theVals.put(names[i], values[i]);
      }
      DateTime ts = new DateTime(timestamp);
      return new MapBasedRow(ts, theVals);
    }
  }

  public static Row createExpectedRow(final String timestamp, Object... vals)
  {
    return createExpectedRow(new DateTime(timestamp), vals);
  }

  public static Row createExpectedRow(final DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0, "invalid row " + Arrays.toString(vals));

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = new DateTime(timestamp);
    return new MapBasedRow(ts, theVals);
  }

  public static List<Row> createExpectedRows(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    List<Row> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < Math.min(value.length, columnNames.length); i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        }
      }
      DateTime timestamp = timeIndex < 0 ? null : new DateTime(value[timeIndex]);
      expected.add(new MapBasedRow(timestamp, theVals));
    }
    return expected;
  }

  public static void validate(String[] columnNames, List<Row> expected, List<Row> resultIterable)
  {
    validate(columnNames, expected, resultIterable, false);
  }

  public static void validate(
      String[] columnNames,
      List<Row> expected,
      List<Row> resultIterable,
      boolean ensureColumnNames
  )
  {
    try {
      _validate(columnNames, expected, resultIterable, ensureColumnNames);
    }
    catch (AssertionError e) {
      printToExpected(columnNames, resultIterable);
      throw e;
    }
  }

  private static void _validate(
      String[] columnNames,
      List<Row> expected,
      List<Row> resultIterable,
      boolean ensureColumnNames
  )
  {
    List<Row> result = Lists.newArrayList(resultIterable);
    int max = Math.min(expected.size(), result.size());
    for (int i = 0; i < max; i++) {
      Row e = expected.get(i);
      Row r = result.get(i);
      if (ArrayUtils.indexOf(columnNames, "__time") >= 0) {
        Assert.assertEquals(i + " th __time", e.getTimestamp(), r.getTimestamp());
      }
      for (String columnName : columnNames) {
        final Object ev = e.getRaw(columnName);
        final Object rv = r.getRaw(columnName);
        if (ev instanceof Float && rv instanceof Double || ev instanceof Double && rv instanceof Float) {
          Assert.assertEquals(((Number) ev).doubleValue(), ((Number) rv).doubleValue(), 0.0001);
        } else if (ev instanceof Long && rv instanceof Integer || ev instanceof Integer && rv instanceof Long) {
          Assert.assertEquals(((Number) ev).longValue(), ((Number) rv).longValue());
        } else if (ev instanceof String && rv instanceof UTF8Bytes || ev instanceof UTF8Bytes && rv instanceof String) {
          Assert.assertEquals(Objects.toString(ev, null), Objects.toString(rv, null));
        } else {
          Assert.assertEquals(i + " th " + columnName, ev, rv);
        }
      }
      if (ensureColumnNames) {
        Assert.assertEquals(e.getColumns().size(), r.getColumns().size());
        Assert.assertTrue(e.getColumns().containsAll(r.getColumns()));
      }
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less");
    }
  }

  public static void printToExpected(String[] columnNames, Iterable<Row> results)
  {
    for (Row x: results) {
      StringBuilder b = new StringBuilder();
      for (String d : columnNames) {
        if (b.length() > 0) {
          b.append(", ");
        }
        if (d.equals("__time")) {
          String timestamp = x.getTimestamp().toString().replace("T00:00:00.000Z", "");
          b.append('"').append(timestamp).append('"');
          continue;
        }
        printTo(x.getRaw(d), b);
      }
      System.out.println("array(" + b + "),");
    }
  }

  public static void printToExpected(List<Object[]> results)
  {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < results.size(); i++) {
      b.setLength(0);
      Object[] x = results.get(i);
      for (int j = 0; j < x.length; j++) {
        if (j > 0) {
          b.append(", ");
        }
        printTo(x[j], b);
      }
      if (i > 0) {
        System.out.println(",");
      }
      System.out.print("{" + b + "}");
    }
    System.out.println();
  }

  private static void printTo(Object o, StringBuilder b)
  {
    if (o == null) {
      b.append("null");
    } else if (o instanceof String) {
      b.append('"').append(o).append('"');
    } else if (o instanceof DateTime) {
      b.append("new DateTime(").append('"').append(o).append('"').append(')');
    } else if (o instanceof Long) {
      b.append(o).append('L');
    } else if (o instanceof Double) {
      b.append(o).append('D');
    } else if (o instanceof Float) {
      b.append(o).append('F');
    } else if (o instanceof List) {
      b.append("list(");
      List l = (List)o;
      for (int i = 0; i < l.size(); i++) {
        if (i > 0) {
          b.append(", ");
        }
        Object e = l.get(i);
        if (e instanceof String) {
          b.append('"').append(e).append('"');
        } else if (e instanceof Long) {
          b.append(e).append('L');
        } else if (e instanceof Double) {
          b.append(e).append('D');
        } else if (e instanceof Float) {
          b.append(e).append('F');
        } else {
          b.append(e);
        }
      }
      b.append(')');
    } else if (o.getClass().isArray()) {
      Class compType = o.getClass().getComponentType();
      if (compType == Long.class || compType == Long.TYPE) {
        b.append("new long[] {");
      } else if (compType == Double.class || compType == Double.TYPE) {
        b.append("new double[] {");
      } else if (compType == Float.class || compType == Float.TYPE) {
        b.append("new float[] {");
      } else if (compType == String.class) {
        b.append("new String[] {");
      } else {
        b.append("new Object[] {");
      }
      int length = Array.getLength(o);
      for (int i = 0; i < length; i++) {
        if (i > 0) {
          b.append(", ");
        }
        Object e = Array.get(o, i);
        if (e instanceof String) {
          b.append('"').append(e).append('"');
        } else if (e instanceof Long) {
          b.append(e).append('L');
        } else if (e instanceof Double) {
          b.append(e).append('D');
        } else if (e instanceof Float) {
          b.append(e).append('F');
        } else {
          b.append(e);
        }
      }
      b.append('}');
    } else {
      b.append(o);
    }
  }
}
