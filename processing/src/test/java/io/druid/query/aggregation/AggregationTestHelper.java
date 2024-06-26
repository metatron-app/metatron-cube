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

package io.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import io.druid.collections.BufferPool;
import io.druid.common.Yielders;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.NoopQueryWatcher;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.groupby.VectorizedGroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.select.StreamQueryEngine;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class provides general utility to test any druid aggregation implementation given raw data,
 * parser spec, aggregator specs and a group-by query.
 * It allows you to create index from raw data, run a group by query on it which simulates query processing inside
 * of a druid cluster exercising most of the features from aggregation and returns the results that you could verify.
 */
public class AggregationTestHelper
{
  private final ObjectMapper mapper;
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;
  private final QueryToolChest toolChest;
  private final QueryRunnerFactory factory;

  private final TemporaryFolder tempFolder;

  private AggregationTestHelper(
      ObjectMapper mapper,
      IndexMerger indexMerger,
      IndexIO indexIO,
      QueryToolChest toolchest,
      QueryRunnerFactory factory,
      TemporaryFolder tempFolder,
      List<? extends Module> jsonModulesToRegister
  )
  {
    this.mapper = mapper;
    this.indexMerger = indexMerger;
    this.indexIO = indexIO;
    this.toolChest = toolchest;
    this.factory = factory;
    this.tempFolder = tempFolder;

    for(Module mod : jsonModulesToRegister) {
      mapper.registerModule(mod);
    }
  }

  public static final AggregationTestHelper createGroupByQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = TestHelper.JSON_MAPPER;

    QueryConfig config = new QueryConfig();
    BufferPool pool = BufferPool.heap(1024 * 1024);

    GroupByQueryEngine engine = new GroupByQueryEngine(pool);
    VectorizedGroupByQueryEngine batch = new VectorizedGroupByQueryEngine(pool);
    StreamQueryEngine stream = new StreamQueryEngine();
    GroupByQueryQueryToolChest toolchest = new GroupByQueryQueryToolChest(config, engine, pool);
    GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        batch,
        stream,
        NoopQueryWatcher.instance(),
        config,
        toolchest
    );

    IndexIO indexIO = new IndexIO(
        mapper
    );

    return new AggregationTestHelper(
        mapper,
        new IndexMerger(mapper, indexIO),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister
    );
  }

  public static AggregationTestHelper createSelectQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    QueryRunnerFactory<Result<SelectResultValue>> factory = TestHelper.factoryFor(SelectQuery.class);

    IndexIO indexIO = new IndexIO(
        mapper
    );

    return new AggregationTestHelper(
        mapper,
        new IndexMerger(mapper, indexIO),
        indexIO,
        factory.getToolchest(),
        factory,
        tempFolder,
        jsonModulesToRegister
    );
  }

  public static final AggregationTestHelper createSearchQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    SearchQueryQueryToolChest toolchest = new SearchQueryQueryToolChest(
        new SearchQueryConfig()
    );

    SearchQueryRunnerFactory factory = new SearchQueryRunnerFactory(
        new SearchQueryQueryToolChest(
            new SearchQueryConfig()
        ),
        TestHelper.NOOP_QUERYWATCHER
    );

    return new AggregationTestHelper(
        mapper,
        TestHelper.getTestIndexMerger(),
        TestHelper.getTestIndexIO(),
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister
    );
  }

  public Sequence<Row> createIndexAndRunQueryOnSegment(
      File inputDataFile,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataFile, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  public Sequence<Row> createIndexAndRunQueryOnSegment(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataStream, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  public void createIndex(
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      Granularity gran,
      int maxRowCount
  ) throws Exception
  {
    createIndex(
        new FileInputStream(inputDataFile),
        parserJson,
        aggregators,
        outDir,
        minTimestamp,
        gran,
        maxRowCount
    );
  }

  public void createIndex(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      Granularity gran,
      int maxRowCount
  ) throws Exception
  {
    try {
      StringInputRowParser parser = mapper.readValue(parserJson, StringInputRowParser.class);

      LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
      List<AggregatorFactory> aggregatorSpecs = mapper.readValue(
          aggregators,
          new TypeReference<List<AggregatorFactory>>()
          {
          }
      );

      createIndex(
          iter,
          parser,
          aggregatorSpecs.toArray(new AggregatorFactory[0]),
          outDir,
          minTimestamp,
          gran,
          true,
          maxRowCount
      );
    }
    finally {
      Closeables.close(inputDataStream, true);
    }
  }

  public void createIndex(
      Iterator rows,
      InputRowParser parser,
      final AggregatorFactory[] metrics,
      File outDir,
      long minTimestamp,
      Granularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount
  ) throws Exception
  {
    IncrementalIndex index = null;
    List<File> toMerge = new ArrayList<>();

    try {
      index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, true, true, maxRowCount);
      while (rows.hasNext()) {
        Object row = rows.next();
        if (!index.canAppendRow()) {
          File tmp = tempFolder.newFolder();
          toMerge.add(tmp);
          indexMerger.persist(index, tmp, IndexSpec.DEFAULT);
          index.close();
          index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, true,
                                             true, maxRowCount);
        }
        index.add(parser.parse(row));
      }

      if (toMerge.size() > 0) {
        File tmp = tempFolder.newFolder();
        toMerge.add(tmp);
        indexMerger.persist(index, tmp, IndexSpec.DEFAULT);

        List<QueryableIndex> indexes = new ArrayList<>(toMerge.size());
        for (File file : toMerge) {
          indexes.add(indexIO.loadIndex(file));
        }
        indexMerger.mergeQueryableIndex(indexes, true, metrics, outDir, IndexSpec.DEFAULT);

        for (QueryableIndex qi : indexes) {
          qi.close();
        }
      } else {
        indexMerger.persist(index, outDir, IndexSpec.DEFAULT);
      }
    }
    finally {
      if (index != null) {
        index.close();
      }
    }
  }

  //Simulates running group-by query on individual segments as historicals would do, json serialize the results
  //from each segment, later deserialize and merge and finally return the results
  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final String queryJson) throws Exception
  {
    return runQueryOnSegments(segmentDirs, mapper.readValue(queryJson, Query.class));
  }

  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final Query query)
  {
    final List<Segment> segments = Lists.transform(
        segmentDirs,
        new Function<File, Segment>()
        {
          @Override
          public Segment apply(File segmentDir)
          {
            try {
              return new QueryableIndexSegment(indexIO.loadIndex(segmentDir), DataSegment.asKey(""));
            }
            catch (IOException ex) {
              throw Throwables.propagate(ex);
            }
          }
        }
    );

    try {
      return runQueryOnSegmentsObjs(segments, query);
    } finally {
      for(Segment segment: segments) {
        CloseQuietly.close(segment);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public Sequence<Row> runQueryOnSegmentsObjs(final List<Segment> segments, final Query<Row> query)
  {
    return QueryRunners.run(query, toolChest.finalQueryDecoration(
        toolChest.finalizeResults(
            toolChest.postMergeQueryDecoration(
                toolChest.mergeResults(
                    toolChest.preMergeQueryDecoration(
                        makeLocalizedRunner(segments, query)
                    )
                )
            )
        )
    ));
  }

  @SuppressWarnings("unchecked")
  private QueryRunner<Row> makeLocalizedRunner(final List<Segment> segments, final Query<Row> query)
  {
    final Query<Row> localized = query.toLocalQuery();
    final Supplier<RowResolver> resolver = RowResolver.supplier(segments, localized);
    final Query<Row> resolved = localized.resolveQuery(resolver, true);

    final QueryRunner<Row> baseRunner = factory.mergeRunners(
        query,
        Execs.newDirectExecutorService(),
        Lists.transform(segments, segment -> factory.createRunner(segment, null)),
        null
    );

    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> map)
      {
        Sequence<Row> sequence = baseRunner.run(resolved, Maps.<String, Object>newHashMap());
        try {
          Yielder yielder = Yielders.each(sequence);
          byte[] resultStr = mapper.writer().writeValueAsBytes(yielder);

          JavaType baseType = toolChest.getResultTypeReference(resolved, mapper.getTypeFactory());

          // in broker
          List resultRows = Lists.transform(
              readQueryResultArrayFromBytes(resultStr, baseType),
              toolChest.makePreComputeManipulatorFn(
                  query,
                  MetricManipulatorFns.deserializing()
              )
          );
          return Sequences.simple(sequence.columns(), resultRows);
        } catch(Exception ex) {
          throw Throwables.propagate(ex);
        }
      }
    };
  }

  private List readQueryResultArrayFromBytes(byte[] str, JavaType baseType) throws Exception
  {
    List result = new ArrayList();

    JsonParser jp = mapper.getFactory().createParser(str);

    if (jp.nextToken() != JsonToken.START_ARRAY) {
      throw new IAE("not an array [%s]", new String(str));
    }

    ObjectCodec objectCodec = jp.getCodec();

    while(jp.nextToken() != JsonToken.END_ARRAY) {
      result.add(objectCodec.readValue(jp, baseType));
    }
    return result;
  }

  public ObjectMapper getObjectMapper()
  {
    return mapper;
  }
}

