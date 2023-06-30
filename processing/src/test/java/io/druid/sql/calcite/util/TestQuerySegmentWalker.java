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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import io.druid.common.DateTimes;
import io.druid.common.Yielders;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.jackson.JsonParserIterator;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.BySegmentResultValue;
import io.druid.query.ConveyQuery;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.LocalStorageHandler;
import io.druid.query.NoopQueryRunner;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.StorageHandler;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.TestLoadSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.server.DruidNode;
import io.druid.server.ForwardHandler;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 */
public class TestQuerySegmentWalker implements ForwardingSegmentWalker, QueryToolChestWarehouse
{
  private static final Logger LOG = new Logger(TestQuerySegmentWalker.class);

  private final ObjectMapper mapper;
  private final IndexIO indexIO;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService executor;
  private final QueryConfig queryConfig;

  private final PopulatingMap timeLines;
  private final ForwardHandler handler;

  private final Consumer<Query<?>> hook;

  public TestQuerySegmentWalker addSalesIndex()
  {
    return addIndex("sales", "sales_schema.json", "sales.tsv", true);
  }

  public TestQuerySegmentWalker addCategoryAliasIndex()
  {
    return addIndex("category_alias", "category_alias_schema.json", "category_alias.tsv", true);
  }

  public TestQuerySegmentWalker addProfileIndex()
  {
    return addIndex("profile", "profile_schema.json", "profile.csv", true);
  }

  public TestQuerySegmentWalker addCdisIndex()
  {
    return addIndex("cdis", "cdis_schema.json", "cdis.tbl", true)
        .addIndex("cdis_i", "cdis_schema.json", "cdis.tbl", false);
  }

  public TestQuerySegmentWalker addTpchIndex()
  {
    return addIndex("lineitem", "lineitem_schema.json", "lineitem.tbl", true)
        .addIndex("orders", "orders_schema.json", "orders.tbl", true)
        .addIndex("customer", "customer_schema.json", "customer.tbl", true)
        .addIndex("nation", "nation_schema.json", "nation.tbl", true)
        .addIndex("part", "part_schema.json", "part.tbl", true)
        .addIndex("partsupp", "partsupp_schema.json", "partsupp.tbl", true)
        .addIndex("region", "region_schema.json", "region.tbl", true)
        .addIndex("supplier", "supplier_schema.json", "supplier.tbl", true);
  }

  public TestQuerySegmentWalker addSsbIndex()
  {
    return addIndex("ssb_lineorder", "ssb_lineorder_schema.json", "ssb_lineorder.tbl", true)
        .addIndex("ssb_part", "ssb_part_schema.json", "ssb_part.tbl", true)
        .addIndex("ssb_customer", "ssb_customer_schema.json", "ssb_customer.tbl", true)
        .addIndex("ssb_date", "ssb_date_schema.json", "ssb_date.tbl", true)
        .addIndex("ssb_supplier", "ssb_supplier_schema.json", "ssb_supplier.tbl", true);
  }

  public synchronized TestQuerySegmentWalker addIndex(
      final String ds,
      final String schemaFile,
      final String sourceFile,
      final boolean mmapped
  )
  {
    TestLoadSpec schema = loadJson(schemaFile, TestLoadSpec.class, mapper);
    return load(ds, schema, () -> asCharSource(sourceFile), mmapped);
  }

  public synchronized void addIndex(
      final String ds,
      final List<String> columns,
      final List<String> types,
      final Granularity segmentGran,
      final String source
  )
  {
    int timeIx = columns.indexOf("time");
    String timeFormat = types.get(timeIx);
    TimestampSpec spec = new DefaultTimestampSpec("time", timeFormat, DateTimes.nowUtc());

    int dimIx = types.lastIndexOf("dimension");
    List<String> dimensions = Lists.newArrayList();
    for (int i = 0; i < dimIx + 1; i++) {
      if (i != timeIx) {
        dimensions.add(columns.get(i));
      }
    }
    DimensionsSpec dimensionsSpec = DimensionsSpec.ofStringDimensions(dimensions);

    List<AggregatorFactory> metrics = Lists.newArrayList();
    for (int i = dimIx + 1; i < columns.size(); i++) {
      if (i != timeIx) {
        metrics.add(new RelayAggregatorFactory(columns.get(i), ValueDesc.of(types.get(i))));
      }
    }
    TestLoadSpec schema = new TestLoadSpec(
        0,
        Granularities.DAY,
        segmentGran,
        ImmutableMap.<String, Object>of("format", "csv"),
        columns,
        spec,
        dimensionsSpec,
        metrics.toArray(new AggregatorFactory[0]),
        null, null, false, false, true, null
    );
    load(ds, schema, () -> CharSource.wrap(source), true);
  }

  private TestQuerySegmentWalker load(
      final String ds,
      final TestLoadSpec schema,
      final Supplier<CharSource> source,
      final boolean mmapped
  )
  {
    addPopulator(
        ds,
        new Supplier<List<Segment>>()
        {
          @Override
          public List<Segment> get()
          {
            final Granularity granularity = schema.getSegmentGran();
            final InputRowParser parser = schema.getParser(mapper, false);

            final List<Segment> segments = Lists.newArrayList();
            final CharSource charSource = source.get();
            try (Reader reader = charSource.openStream()) {
              final Iterator<InputRow> rows = readRows(reader, parser);
              final Map<Long, IncrementalIndex> indices = Maps.newHashMap();
              while (rows.hasNext()) {
                InputRow inputRow = rows.next();
                DateTime dateTime = granularity.bucketStart(inputRow.getTimestamp());
                IncrementalIndex index = indices.computeIfAbsent(
                    dateTime.getMillis(),
                    timestamp -> new OnheapIncrementalIndex(schema, true, 100000)
                );
                index.add(inputRow);
              }
              for (Map.Entry<Long, IncrementalIndex> entry : indices.entrySet()) {
                Long instant = entry.getKey();
                IncrementalIndex index = entry.getValue();
                Interval interval = new Interval(instant, granularity.bucketEnd(entry.getKey()));
                DataSegment segmentSpec = new DataSegment(
                    ds, interval, "0", null, schema.getDimensionNames(), schema.getMetricNames(), null, null, 0
                );
                Segment segment = mmapped ? new QueryableIndexSegment(
                    TestHelper.persistRealtimeAndLoadMMapped(index, schema.getIndexingSpec(), indexIO), segmentSpec) :
                                  new IncrementalIndexSegment(index, segmentSpec);
                segments.add(segment);
              }
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
            return segments;
          }
        }
    );
    return this;
  }

  private static <T> T loadJson(String resource, Class<T> reference, ObjectMapper mapper)
  {
    try {
      return mapper.readValue(asCharSource(resource).openStream(), reference);
    }
    catch (Exception e) {
      throw new RuntimeException("failed to load " + resource, e);
    }
  }

  private static CharSource asCharSource(String resourceFilename)
  {
    return TestIndex.asCharSource(Thread.currentThread().getContextClassLoader(), resourceFilename);
  }

  @SuppressWarnings("unchecked")
  private static Iterator<InputRow> readRows(final Reader reader, final InputRowParser parser) throws IOException
  {
    if (parser instanceof InputRowParser.Streaming) {
      InputRowParser.Streaming streaming = ((InputRowParser.Streaming) parser);
      if (streaming.accept(reader)) {
        return streaming.parseStream(reader);
      }
    }
    return Iterators.transform(
        CharStreams.readLines(reader).iterator(),
        new com.google.common.base.Function<String, InputRow>()
        {
          @Override
          public InputRow apply(String input)
          {
            return parser.parse(input);
          }
        }
    );
  }

  public synchronized TestQuerySegmentWalker addIndex(String ds, String resourceHome, IntStream shards)
  {
    final URL resource = Thread.currentThread().getContextClassLoader().getResource(resourceHome);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceHome);
    }
    LOG.info("Loading from resource [%s]", resource);
    addPopulator(ds,
        () -> {
          final IndexIO indexIO = TestHelper.getTestIndexIO();
          final List<Segment> segments = Lists.newArrayList();
          try {
            File file = new File(resource.getPath());

            PrimitiveIterator.OfInt iterator = shards.iterator();
            while (iterator.hasNext()) {
              int i = iterator.next();
              QueryableIndex index = indexIO.loadIndex(new File(file, String.valueOf(i)));
              DataSegment segment = new DataSegment(
                  ds,
                  index.getInterval(),
                  "0",
                  null,
                  Lists.newArrayList(index.getAvailableDimensions()),
                  Lists.newArrayList(index.getAvailableMetrics()),
                  LinearShardSpec.of(i),
                  null,
                  index.getSerializedSize()
              );
              segments.add(new QueryableIndexSegment(index, segment));
            }
            return segments;
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
    );
    return this;
  }

  @Override
  public StorageHandler getHandler(String scheme)
  {
    return handler.getHandler(scheme);
  }

  @Override
  public <T> QueryRunner<T> handle(Query<T> query, QueryRunner<T> baseRunner)
  {
    return handler.wrapForward(query, baseRunner);
  }

  public ForwardHandler getForwardHandler()
  {
    return handler;
  }

  public PopulatingMap getTimeLines()
  {
    return timeLines;
  }

  public static class PopulatingMap
  {
    private final List<DataSegment> segments = Lists.newArrayList();
    private final Map<String, VersionedIntervalTimeline<Segment>> node1 = Maps.newHashMap();
    private final Map<String, VersionedIntervalTimeline<Segment>> node2 = Maps.newHashMap();
    private final Map<String, Supplier<List<Segment>>> populators = Maps.newHashMap();

    public Set<String> getDataSource()
    {
      return Sets.newHashSet(Iterables.concat(node1.keySet(), node2.keySet(), populators.keySet()));
    }

    private void addSegment(DataSegment descriptor, Segment segment)
    {
      int node = segment.getInterval().hashCode() % 2;
      VersionedIntervalTimeline<Segment> timeline = get(descriptor.getDataSource(), node);
      timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpecWithDefault().createChunk(segment));
      segments.add(descriptor);
    }

    public VersionedIntervalTimeline<Segment> get(String key, int node)
    {
      Supplier<List<Segment>> populator = populators.remove(key);
      if (populator == null) {
        return (node == 0 ? node1 : node2).computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      }
      populate(key, populator);
      return (node == 0 ? node1 : node2).get(key);
    }

    private boolean populate(String key)
    {
      Supplier<List<Segment>> populator = populators.remove(key);
      if (populator != null) {
        populate(key, populator);
        return true;
      }
      return false;
    }

    private void populate(String key, Supplier<List<Segment>> populator)
    {
      VersionedIntervalTimeline<Segment> timeline1 = node1.computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      VersionedIntervalTimeline<Segment> timeline2 = node2.computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      for (Segment segment : populator.get()) {
        DataSegment descriptor = segment.getDescriptor();
        if (descriptor.getInterval().hashCode() % 2 == 0) {
          timeline1.add(
              descriptor.getInterval(),
              descriptor.getVersion(),
              descriptor.getShardSpecWithDefault().createChunk(segment)
          );
        } else {
          timeline2.add(
              descriptor.getInterval(),
              descriptor.getVersion(),
              descriptor.getShardSpecWithDefault().createChunk(segment)
          );
        }
        segments.add(descriptor);
      }
    }

    public void addPopulator(String key, Supplier<List<Segment>> populator)
    {
      Preconditions.checkArgument(!populators.containsKey(key));
      populators.put(key, Suppliers.memoize(populator));
    }
  }

  public TestQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this(TestHelper.JSON_MAPPER, conglomerate, Execs.multiThreaded(4, "exec-%d"), new PopulatingMap(), q -> {});
  }

  private TestQuerySegmentWalker(
      ObjectMapper mapper,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService executor,
      PopulatingMap timeLines,
      Consumer<Query<?>> hook
  )
  {
    this.mapper = mapper;
    this.indexIO = new IndexIO(mapper);
    this.conglomerate = conglomerate;
    this.executor = executor;
    this.queryConfig = conglomerate.getConfig();
    this.timeLines = timeLines;
    this.handler = new ForwardHandler(
        new DruidNode("test", "test", 0),
        mapper,
        asWarehouse(queryConfig, conglomerate),
        GuavaUtils.mutableMap("file", new LocalStorageHandler(mapper)),
        this
    );
    this.hook = hook;
  }

  public TestQuerySegmentWalker withConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    return new TestQuerySegmentWalker(
        mapper,
        conglomerate,
        executor,
        timeLines,
        hook
    );
  }

  public TestQuerySegmentWalker withObjectMapper(ObjectMapper objectMapper)
  {
    return new TestQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        timeLines,
        hook
    );
  }

  public TestQuerySegmentWalker withExecutor(ExecutorService executor)
  {
    return new TestQuerySegmentWalker(
        mapper,
        conglomerate,
        executor,
        timeLines,
        hook
    );
  }

  public TestQuerySegmentWalker withQueryHook(Consumer<Query<?>> hook)
  {
    return new TestQuerySegmentWalker(
        mapper,
        conglomerate,
        executor,
        timeLines,
        hook
    );
  }

  public TestQuerySegmentWalker duplicate()
  {
    PopulatingMap duplicate = new PopulatingMap();
    duplicate.segments.addAll(timeLines.segments);
    duplicate.node1.putAll(timeLines.node1);
    duplicate.node2.putAll(timeLines.node2);
    duplicate.populators.putAll(timeLines.populators);
    return new TestQuerySegmentWalker(
        mapper.copy(),
        TestHelper.newConglometator(),
        executor,
        duplicate,
        hook
    );
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return conglomerate;
  }

  @Override
  public QueryConfig getConfig()
  {
    return queryConfig;
  }

  @Override
  public ExecutorService getExecutor()
  {
    return executor;
  }

  public TestQuerySegmentWalker add(DataSegment descriptor, IncrementalIndex index)
  {
    timeLines.addSegment(descriptor, new IncrementalIndexSegment(index, descriptor));
    return this;
  }

  public TestQuerySegmentWalker add(DataSegment descriptor, QueryableIndex index)
  {
    timeLines.addSegment(descriptor, new QueryableIndexSegment(index, descriptor));
    return this;
  }

  public boolean populate(String dataSource)
  {
    return timeLines.populate(dataSource);
  }

  public void addPopulator(String dataSource, Supplier<List<Segment>> populator)
  {
    timeLines.addPopulator(dataSource, populator);
  }

  public List<DataSegment> getSegments()
  {
    return timeLines.segments;
  }

  @SuppressWarnings("unchecked")
  private <T> Query<T> prepareQuery(Query<T> query)
  {
    String queryId = query.getId() == null ? UUID.randomUUID().toString() : query.getId();
    query = QueryUtils.readPostProcessors(query, mapper);
    query = QueryUtils.setQueryId(query, queryId);
    query = QueryUtils.rewriteRecursively(query, this);
    query = QueryUtils.resolveRecursively(query, this);
    return query;
  }

  @Override
  public ObjectMapper getMapper()
  {
    return mapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeQueryRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeQueryRunner(query);
  }

  private <T> QueryRunner<T> makeQueryRunner(Query<T> query)
  {
    hook.accept(query);
    Query<T> prepared = prepareQuery(query);
    return QueryRunners.runWith(prepared, toQueryRunner(prepared));
  }

  private <T> Iterable<Pair<SegmentDescriptor, Segment>> getSegment(Query<T> input, int node)
  {
    final String dataSourceName = Iterables.getOnlyElement(input.getDataSource().getNames());
    final VersionedIntervalTimeline<Segment> timeline = timeLines.get(dataSourceName, node);
    if (timeline == null) {
      return ImmutableList.of();
    }
    QuerySegmentSpec segmentSpec = input.getQuerySegmentSpec();
    if (segmentSpec instanceof MultipleSpecificSegmentSpec) {
      List<SegmentDescriptor> segments = ((MultipleSpecificSegmentSpec) segmentSpec).getDescriptors();
      return Iterables.transform(
          segments, new Function<SegmentDescriptor, Pair<SegmentDescriptor, Segment>>()
          {
            @Override
            public Pair<SegmentDescriptor, Segment> apply(SegmentDescriptor input)
            {
              PartitionHolder<Segment> entry = timeline.findEntry(
                  input.getInterval(), input.getVersion()
              );
              if (entry != null) {
                PartitionChunk<Segment> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk != null) {
                  return Pair.of(input, chunk.getObject());
                }
              }
              return Pair.of(input, null);
            }
          }
      );
    }
    return Iterables.concat(
        Iterables.transform(
            Iterables.concat(
                Iterables.transform(
                    input.getIntervals(),
                    new Function<Interval, Iterable<TimelineObjectHolder<Segment>>>()
                    {
                      @Override
                      public Iterable<TimelineObjectHolder<Segment>> apply(Interval input)
                      {
                        return timeline.lookup(input);
                      }
                    }
                )
            ),
            new Function<TimelineObjectHolder<Segment>, Iterable<Pair<SegmentDescriptor, Segment>>>()
            {
              @Override
              public Iterable<Pair<SegmentDescriptor, Segment>> apply(
                  @Nullable final TimelineObjectHolder<Segment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return Iterables.transform(
                    holder.getObject(),
                    new Function<PartitionChunk<Segment>, Pair<SegmentDescriptor, Segment>>()
                    {
                      @Override
                      public Pair<SegmentDescriptor, Segment> apply(PartitionChunk<Segment> chunk)
                      {
                        return Pair.of(
                            new SegmentDescriptor(
                                dataSourceName,
                                holder.getInterval(),
                                holder.getVersion(),
                                chunk.getChunkNumber()
                            ), chunk.getObject()
                        );
                      }
                    }
                );
              }
            }
        )
    );
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> toQueryRunner(final Query<T> query)
  {
    if (query instanceof ConveyQuery) {
      return QueryRunners.wrap(((ConveyQuery<T>) query).getValues());
    }
    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    final QueryToolChest<T> toolChest = factory == null ? null : factory.getToolchest();
    if (query.getDataSource() instanceof QueryDataSource) {
      Preconditions.checkNotNull(factory, "%s does not supports nested query", query);
      QueryRunner<T> runner = toolChest.handleSubQuery(this);
      return FluentQueryRunnerBuilder.create(toolChest, runner)
                                     .applyFinalizeResults()
                                     .applyFinalQueryDecoration()
                                     .applyPostProcessingOperator()
                                     .applySubQueryResolver(this)
                                     .runWith(query)
                                     .build();
    }
    if (query instanceof UnionAllQuery) {
      return QueryRunners.runWith(query, ((UnionAllQuery) query).getUnionQueryRunner(this, queryConfig));
    }
    if (query instanceof Query.IteratingQuery) {
      QueryRunner runner = QueryRunners.getIteratingRunner((Query.IteratingQuery) query, this);
      return FluentQueryRunnerBuilder.create(toolChest, runner)
                                     .applyFinalizeResults()
                                     .applyFinalQueryDecoration()
                                     .applyPostProcessingOperator()
                                     .runWith(query)
                                     .build();
    }
    if (factory == null) {
      return PostProcessingOperators.wrap(NoopQueryRunner.instance());
    }

    // things done in CCC
    QueryRunner<T> runner = new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        query = QueryUtils.decompress(query);
        return QueryUtils.mergeSort(query, Arrays.asList(
            toDataQueryRunner(query, getSegment(query, 0)).run(query, responseContext),
            toDataQueryRunner(query, getSegment(query, 1)).run(query, responseContext)
        ));
      }
    };
    if (!BaseQuery.isBrokerSide(query)) {
      return runner;
    }

    return FluentQueryRunnerBuilder.create(factory.getToolchest(), runner)
                                   .runWithLocalized(this)
                                   .applyPreMergeDecoration()
                                   .applyMergeResults()
                                   .applyPostMergeDecoration()
                                   .applyFinalizeResults()
                                   .applyFinalQueryDecoration()
                                   .applyPostProcessingOperator()
                                   .runWith(query)
                                   .build();
  }

  private <T> QueryRunner<T> toDataQueryRunner(Query<T> query, Iterable<Pair<SegmentDescriptor, Segment>> segments)
  {
    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    final QueryToolChest<T> toolChest = factory.getToolchest();

    List<Segment> targets = Lists.newArrayList();
    List<SegmentDescriptor> descriptors = Lists.newArrayList();
    List<QueryRunner<T>> missingSegments = Lists.newArrayList();
    for (Pair<SegmentDescriptor, Segment> segment : segments) {
      if (segment.rhs != null) {
        targets.add(Segments.withLimit(segment.rhs, segment.lhs));
      } else {
        missingSegments.add(new ReportTimelineMissingSegmentQueryRunner<T>(segment.lhs));
      }
      descriptors.add(segment.lhs);
    }

    if (!(query.getQuerySegmentSpec() instanceof MultipleSpecificSegmentSpec)) {
      query = query.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(descriptors));
    }
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    if (targets.isEmpty()) {
      return PostProcessingOperators.wrap(QueryRunners.empty());
    }
    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver, true);
    final Supplier<Object> optimizer = factory.preFactoring(resolved, targets, resolver, executor);

    final Function<Iterable<Segment>, QueryRunner<T>> function = new Function<Iterable<Segment>, QueryRunner<T>>()
    {
      @Override
      public QueryRunner<T> apply(Iterable<Segment> segments)
      {
        Iterable<QueryRunner<T>> runners = Iterables.transform(
            segments,
            segment -> new SpecificSegmentQueryRunner<T>(
                new BySegmentQueryRunner<T>(
                    toolChest,
                    segment.getIdentifier(),
                    segment.getInterval().getStart(),
                    factory.createRunner(segment, optimizer)
                ),
                segment.asSpec()
            )
        );
        return QueryRunners.finalizeAndPostProcessing(
            toolChest.mergeResults(
                factory.mergeRunners(resolved, executor, runners, optimizer)
            ),
            toolChest,
            mapper
        );
      }
    };

    QueryRunnerFactory.Splitable<T> splitable = null;
    if (factory instanceof QueryRunnerFactory.Splitable) {
      splitable = (QueryRunnerFactory.Splitable<T>) factory;
    }

    if (splitable != null) {
      List<List<Segment>> splits = splitable.splitSegments(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        QueryRunner<T> concat = QueryRunners.concat(Iterables.concat(
            missingSegments, Iterables.transform(splits, function)
        ));
        return serde(concat, this, toolChest);
      }
    }

    QueryRunner<T> runner = QueryRunners.concat(GuavaUtils.concat(missingSegments, function.apply(targets)));
    if (splitable != null) {
      List<Query<T>> splits = splitable.splitQuery(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        runner = QueryRunners.concat(runner, splits);
      }
    }
    return serde(runner, this, toolChest);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> serde(QueryRunner<T> runner, QuerySegmentWalker segmentWalker, QueryToolChest<T> toolChest)
  {
    return (query, response) -> {
      Sequence<T> sequence = runner.run(query, response);
      try {
        BytesOutputStream output = new BytesOutputStream();
        Yielder yielder = Yielders.each(toolChest.serializeSequence(query, sequence, segmentWalker));
        mapper.writer().writeValue(output, yielder);

        JavaType typeReference = toolChest.getResultTypeReference(query, mapper.getTypeFactory());
        JsonParserIterator iterator = new JsonParserIterator(
            mapper, typeReference, "test", "historical", () -> new BytesInputStream(output.toByteArray())
        );
        if (!BaseQuery.isBySegment(query)) {
          return toolChest.deserializeSequence(query, Sequences.once(sequence.columns(), iterator), executor);
        }
        return (Sequence) Sequences.map(
            Sequences.once(sequence.columns(), iterator),
            BySegmentResultValue.applyAll(
                toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing())
            )
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    };
  }

  @Override
  public QueryConfig getQueryConfig()
  {
    return queryConfig;
  }

  @Override
  public <T> QueryToolChest<T> getToolChest(Query<?> query)
  {
    QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    return factory == null ? null : factory.getToolchest();
  }

  private static QueryToolChestWarehouse asWarehouse(
      final QueryConfig queryConfig,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return new QueryToolChestWarehouse()
    {
      @Override
      public QueryConfig getQueryConfig()
      {
        return queryConfig;
      }

      @Override
      public <T> QueryToolChest<T> getToolChest(Query<?> query)
      {
        final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
        return factory == null ? null : factory.getToolchest();
      }
    };
  }
}
