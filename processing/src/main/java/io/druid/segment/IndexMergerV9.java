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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.SerializerUtils;
import io.druid.data.ValueDesc;
import io.druid.granularity.GranularityType;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.ColumnPartWriter.Compressed;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.CompressedVSizeIntWriter;
import io.druid.segment.data.CompressedVSizeIntsV3Writer;
import io.druid.segment.data.CumulativeBitmapWriter;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIntWriter;
import io.druid.segment.data.VSizeIntsWriter;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.StringMetricSerde;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IndexMergerV9 extends IndexMerger
{
  private static final Logger log = new Logger(IndexMergerV9.class);

  @Inject
  public IndexMergerV9(
      ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    super(mapper, indexIO);
  }

  @Override
  protected File makeIndexFiles(
      final List<IndexableAdapter> adapters,
      final AggregatorFactory[] metricAggs,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>> rowMergerFn,
      final int[][] rowNumConversions,
      final IndexSpec indexSpec,
      final boolean rollup
  ) throws IOException
  {
    progress.start();
    progress.progress();

    List<Metadata> metadataList = Lists.transform(adapters, IndexableAdapter::getMetadata);

    AggregatorFactory[] combiningMetricAggs = null;
    if (metricAggs != null) {
      combiningMetricAggs = AggregatorFactory.toCombinerFactory(metricAggs);
    }
    Metadata segmentMetadata = Metadata.merge(metadataList, combiningMetricAggs);

    final Closer closer = Closer.create();
    final IOPeon ioPeon = closer.register(new TmpFileIOPeon(false));
    final FileSmoosher v9Smoosher = new FileSmoosher(outDir);
    final File v9TmpDir = new File(outDir, "v9-tmp");
    v9TmpDir.mkdirs();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        FileUtils.deleteDirectory(v9TmpDir);
      }
    });
    log.info("Start making v9 index files from %d indices, outDir:%s", adapters.size(), outDir);
    try {
      long startTime = System.currentTimeMillis();
      Files.asByteSink(new File(outDir, "version.bin")).write(Ints.toByteArray(IndexIO.V9_VERSION));
      log.info("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

      final Map<String, ValueDesc> metricTypeNames = Maps.newTreeMap(GuavaUtils.nullFirstNatural());
      final List<ColumnCapabilities> dimCapabilities = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeCapabilities(adapters, mergedDimensions, metricTypeNames, dimCapabilities);

      /************* Setup Dim Conversions **************/
      startTime = System.currentTimeMillis();
      final Map<String, ColumnPartSerde> dictionaryFSTs = Maps.newHashMap();
      final Map<String, Integer> dimCardinalities = Maps.newHashMap();
      final ArrayList<GenericIndexedWriter<String>> dictionaryWriters = setupDictionaryWriters(ioPeon, mergedDimensions);
      final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(adapters.size());
      final boolean[] dimensionSkipFlag = new boolean[mergedDimensions.size()];
      final boolean[] dimHasNullFlags = new boolean[mergedDimensions.size()];
      final boolean[] convertMissingDimsFlags = new boolean[mergedDimensions.size()];

       writeDimValueAndSetupDimConversion(
          adapters, progress, mergedDimensions, dictionaryWriters, dictionaryFSTs, dimCardinalities,
          dimensionSkipFlag, dimConversions, convertMissingDimsFlags, dimHasNullFlags, indexSpec
      );
      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
      final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();

      /************* Walk through data sets, merge them, and write merged columns *************/
      final LongColumnSerializer timeWriter = setupTimeWriter(ioPeon, "little_end_time", indexSpec);
      final ArrayList<ColumnPartWriter> dimWriters = setupDimensionWriters(
          ioPeon, mergedDimensions, dimCapabilities, dimCardinalities, indexSpec
      );
      final ArrayList<MetricColumnSerializer> metWriters = setupMetricsWriters(
          ioPeon, mergedMetrics, metricTypeNames, indexSpec
      );
      final ArrayList<MutableBitmap> nullRowsList = Lists.newArrayListWithCapacity(mergedDimensions.size());
      for (int i = 0; i < mergedDimensions.size(); ++i) {
        nullRowsList.add(bitmapFactory.makeEmptyMutableBitmap());
      }
      final Iterator<Rowboat> rows = makeRowIterable(
          adapters, mergedDimensions, mergedMetrics, dimConversions, convertMissingDimsFlags, rowMergerFn
      );
      writeRowValues(
          progress, rows, timeWriter, dimWriters, metWriters, dimensionSkipFlag, nullRowsList, dimHasNullFlags
      );

      /************ Create Inverted Indexes *************/
      final ArrayList<ColumnPartWriter<ImmutableBitmap>> bitmapIndexWriters = setupBitmapIndexWriters(
          ioPeon, mergedDimensions, dimCardinalities, indexSpec
      );
      final ArrayList<ColumnPartWriter<ImmutableRTree>> spatialIndexWriters = setupSpatialIndexWriters(
          ioPeon, mergedDimensions, indexSpec, dimCapabilities
      );
      final ArrayList<Indexed.Closeable<String>> dimValues = Lists.newArrayList();
      for (GenericIndexedWriter<String> dictionaryWriter : dictionaryWriters) {
        dimValues.add(dictionaryWriter.asIndexed());
      }
      makeInvertedIndexes(
          adapters, progress, mergedDimensions, indexSpec, v9TmpDir, rowNumConversions,
          nullRowsList, dimValues, bitmapIndexWriters, spatialIndexWriters, dimConversions
      );

      /************ Finalize Build Columns *************/
      makeTimeColumn(v9Smoosher, progress, Column.TIME_COLUMN_NAME, timeWriter, true);
      makeMetricsColumns(v9Smoosher, progress, mergedMetrics, metricTypeNames, metWriters, true);
      makeDimensionColumns(
          v9Smoosher, progress, indexSpec, mergedDimensions, dimensionSkipFlag, dimCapabilities,
          dictionaryWriters, dictionaryFSTs, dimWriters, bitmapIndexWriters, spatialIndexWriters, true
      );

      Interval dataInterval = toDataInterval(adapters);

      if (!GuavaUtils.isNullOrEmpty(indexSpec.getCuboidSpecs())) {
        // make temporary segment for cube
        SmooshedFileMapper smooshedFiles = v9Smoosher.asMapped(outDir);
        Map<String, Supplier<Column>> columns = Maps.newHashMap();
        for (String columnName : GuavaUtils.concat(
            Arrays.<String>asList(Column.TIME_COLUMN_NAME), mergedMetrics, mergedDimensions)) {
          ByteBuffer mapped = smooshedFiles.mapFile(columnName);
          ColumnDescriptor descriptor = mapper.readValue(SerializerUtils.readString(mapped), ColumnDescriptor.class);
          columns.put(columnName, Suppliers.ofInstance(descriptor.read(columnName, mapped, bitmapSerdeFactory)));
        }
        Indexed<String> dimNames = ListIndexed.ofString(mergedDimensions);
        Indexed<String> columnNames = ListIndexed.ofString(GuavaUtils.concat(mergedMetrics, mergedDimensions));
        SimpleQueryableIndex index = new SimpleQueryableIndex(
            dataInterval, columnNames, dimNames, bitmapFactory, columns, null, smooshedFiles, segmentMetadata
        );
        QueryableIndexSegment segment = new QueryableIndexSegment(index, DataSegment.asKey(dataInterval.toString()));

        StupidPool<ByteBuffer> heapPool = StupidPool.heap(Cuboids.BUFFER_SIZE, Cuboids.PAGES);
        GroupByQueryEngine engine = new GroupByQueryEngine(heapPool);

        for (CuboidSpec cuboidSpec : indexSpec.getCuboidSpecs()) {
          final GranularityType granularity = cuboidSpec.getGranularity();
          final int[] cubeDimIndices = cuboidSpec.toDimensionIndices(mergedDimensions);
          final BigInteger cubeId = Cuboids.toCubeId(cubeDimIndices, granularity);

          log.info("Start making cube %d : %s", cubeId, cuboidSpec);
          long cubeStart = System.currentTimeMillis();
          GroupByQuery.Builder builder = GroupByQuery.builder();
          builder.dataSource("$$cube$$")
                 .intervals(dataInterval)
                 .granularity(granularity.getDefaultGranularity());

          ArrayList<String> cubeDims = Lists.newArrayList();
          for (int cubeDimIndex : cubeDimIndices) {
            String cubeDim = mergedDimensions.get(cubeDimIndex);
            String cubeDimName = Cuboids.dimension(cubeId, cubeDim);
            cubeDims.add(cubeDimName);
            builder.addDimension(cubeDim);
            dimCardinalities.put(cubeDimName, dimCardinalities.get(cubeDim));   // shares dictionary
          }
          builder.addAggregators(
              CountAggregatorFactory.of(Cuboids.metric(cubeId, Cuboids.COUNT_ALL_METRIC, "count"))
          );
          // disable compression for apex cuboid (maybe some threshold like cardinality < 100 ?)
          IndexSpec indexer = indexSpec;
          if (cubeDimIndices.length == 0 &&
              granularity.getDefaultGranularity().count(dataInterval) < Cuboids.METRIC_COMPRESSION_DISABLE) {
            indexer = indexSpec.withMetricCompression("none");
          }

          String timeFileBase = Cuboids.dimension(cubeId, "little_end_time");
          LongColumnSerializer cubeTimeWriter = setupTimeWriter(ioPeon, timeFileBase, indexer);

          ArrayList<ColumnPartWriter> cubeDimWriters = setupDimensionWriters(
              ioPeon, cubeDims, null, dimCardinalities, indexer
          );

          if (rollup) {
            Map<String, Set<String>> metrics = cuboidSpec.getMetrics();
            for (AggregatorFactory factory : segmentMetadata.getAggregators()) {
              String cubeMet = factory.getName();
              if (!metrics.isEmpty() && !metrics.containsKey(cubeMet)) {
                continue;
              }
              String aggregator = Cuboids.cubeName(factory);
              if (aggregator == null) {
                continue;
              }
              ValueDesc cubeMetType = metricTypeNames.get(cubeMet);
              String name = Cuboids.metric(cubeId, factory.getName(), aggregator);
              factory = Cuboids.convert(aggregator, name, cubeMet, cubeMetType);
              if (factory != null) {
                builder.addAggregators(factory);
              }
            }
          } else {
            for (Map.Entry<String, Set<String>> metrics : cuboidSpec.getMetrics().entrySet()) {
              String cubeMet = metrics.getKey();
              ValueDesc cubeMetType = metricTypeNames.get(cubeMet);
              if (cubeMetType == null &&
                  mergedDimensions.indexOf(cubeMet) >= 0 &&
                  !dimCapabilities.get(mergedDimensions.indexOf(cubeMet)).hasMultipleValues()) {
                cubeMetType = ValueDesc.STRING;
              }
              if (cubeMetType == null) {
                log.warn("Skipping cube metric.. [%s]", cubeMet);
                continue;
              }
              Set<String> aggregators = metrics.getValue();
              if (aggregators.isEmpty()) {
                aggregators = Cuboids.BASIC_AGGREGATORS;
              }
              for (String aggregator : aggregators) {
                String name = Cuboids.metric(cubeId, cubeMet, aggregator);
                AggregatorFactory factory = Cuboids.convert(aggregator, name, cubeMet, cubeMetType);
                if (factory != null) {
                  builder.addAggregators(factory);
                }
              }
            }
          }

          GroupByQuery query = builder.build();
          if (query.getAggregatorSpecs().isEmpty()) {
            continue;
          }
          Iterator<Rowboat> cubeRows = Sequences.toIterator(engine.processRowboat(query, segment));

          ArrayList<String> cubeMetrics = Lists.newArrayList();
          Map<String, ValueDesc> cubeMetricTypes = Maps.newHashMap();
          ArrayList<MetricColumnSerializer> cubeMetricWriters = Lists.newArrayList();
          for (AggregatorFactory aggregator : query.getAggregatorSpecs()) {
            cubeMetricWriters.add(
                setupMetricsWriter(ioPeon, aggregator.getName(), aggregator.getOutputType(), indexer)
            );
            cubeMetrics.add(aggregator.getName());
            cubeMetricTypes.put(aggregator.getName(), aggregator.getOutputType());
          }

          ArrayList<ColumnPartWriter<ImmutableBitmap>> cubeBitmapWriters = setupBitmapIndexWriters(
              ioPeon, cubeDims, ImmutableMap.of(), indexer
          );
          MutableBitmap[][] bitmaps = new MutableBitmap[cubeDims.size()][];
          for (int dimId = 0; dimId < bitmaps.length; dimId++) {
            bitmaps[dimId] = new MutableBitmap[dimCardinalities.get(cubeDims.get(dimId))];
            for (int j = 0; j < bitmaps[dimId].length; j++) {
              bitmaps[dimId][j] = bitmapFactory.makeEmptyMutableBitmap();
            }
          }
          // make inverted index here
          writeCubeRowValues(progress, cubeRows, cubeTimeWriter, cubeDimWriters, cubeMetricWriters, bitmaps);

          for (int dimIndex = 0; dimIndex < cubeDims.size(); dimIndex++) {
            final ColumnPartWriter<ImmutableBitmap> bitmapWriter = cubeBitmapWriters.get(dimIndex);
            for (MutableBitmap bitmap : bitmaps[dimIndex]) {
              bitmapWriter.add(bitmapFactory.makeImmutableBitmap(bitmap));
            }
          }

          String timeColumn = Cuboids.dimension(cubeId, Column.TIME_COLUMN_NAME);
          makeTimeColumn(v9Smoosher, progress, timeColumn, cubeTimeWriter, false);
          makeMetricsColumns(v9Smoosher, progress, cubeMetrics, cubeMetricTypes, cubeMetricWriters, false);
          makeDimensionColumns(
              v9Smoosher, progress, indexer,
              cubeDims, null, null, null, null, cubeDimWriters, cubeBitmapWriters, null, false
          );
          log.info("Completed writing cube %d in %,d msec", cubeId, System.currentTimeMillis() - cubeStart);
        }

        heapPool.clear();
        segment.close();
        smooshedFiles.close();
      }

      /************* Make index.drd & metadata.drd files **************/
      makeIndexBinary(
          v9Smoosher, progress, dataInterval, outDir, mergedDimensions, dimensionSkipFlag, mergedMetrics, indexSpec
      );
      if (segmentMetadata != null) {
        makeMetadataBinary(v9Smoosher, progress, segmentMetadata);
      }

      v9Smoosher.close();
      progress.stop();

      long totalSize = 0L;
      for (File file : Preconditions.checkNotNull(outDir.listFiles(), "output dir is not valid")) {
        if (file.isFile() && file.getName().endsWith(".smoosh")) {
          totalSize += file.length();
        }
      }
      log.info("Completed writing index %,d msec, %,d bytes", System.currentTimeMillis() - startTime, totalSize);
      return outDir;
    }
    finally {
      closer.close();
    }
  }

  private void makeIndexBinary(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Interval dataInterval,
      final File outDir,
      final List<String> mergedDimensions,
      final boolean[] dimensionSkipFlag,
      final List<String> mergedMetrics,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final Set<String> finalDimensions = Sets.newLinkedHashSet();
    final Set<String> finalColumns = Sets.newLinkedHashSet();
    finalColumns.addAll(mergedMetrics);
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      if (dimensionSkipFlag[i]) {
        continue;
      }
      finalColumns.add(mergedDimensions.get(i));
      finalDimensions.add(mergedDimensions.get(i));
    }

    GenericIndexed<String> cols = GenericIndexed.v2(finalColumns, ObjectStrategy.STRING_STRATEGY);
    GenericIndexed<String> dims = GenericIndexed.v2(finalDimensions, ObjectStrategy.STRING_STRATEGY);

    final String bitmapSerdeFactoryType = mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory());
    final long numBytes = cols.getSerializedSize()
                          + dims.getSerializedSize()
                          + Long.BYTES * 2
                          + SerializerUtils.getSerializedStringByteSize(bitmapSerdeFactoryType);

    try (SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes)) {
      cols.writeToChannel(writer);
      dims.writeToChannel(writer);

      SerializerUtils.writeLong(writer, dataInterval.getStartMillis());
      SerializerUtils.writeLong(writer, dataInterval.getEndMillis());
      SerializerUtils.writeString(writer, bitmapSerdeFactoryType);
    }

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.info("Completed index.drd in %,d millis.", System.currentTimeMillis() - startTime);

    progress.stopSection(section);
  }

  private void makeMetadataBinary(FileSmoosher v9Smoosher, ProgressIndicator progress, Metadata segmentMetadata)
      throws IOException
  {
    progress.startSection("make metadata.drd");
    v9Smoosher.add("metadata.drd", ByteBuffer.wrap(mapper.writeValueAsBytes(segmentMetadata)));
    progress.stopSection("make metadata.drd");
  }

  private void makeDimensionColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final IndexSpec indexSpec,
      final List<String> mergedDimensions,
      final boolean[] dimensionSkipFlag,
      final List<ColumnCapabilities> dimCapabilities,
      final List<GenericIndexedWriter<String>> dictionaryWriters,
      final Map<String, ColumnPartSerde> dictionaryFSTs,
      final List<ColumnPartWriter> dimWriters,
      final List<ColumnPartWriter<ImmutableBitmap>> bitmapIndexWriters,
      final List<ColumnPartWriter<ImmutableRTree>> spatialIndexWriters,
      final boolean includeStats
  ) throws IOException
  {
    final String section = "make dimension columns";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      if (dimensionSkipFlag != null && dimensionSkipFlag[i]) {
        continue;
      }
      long dimStartTime = System.currentTimeMillis();
      final String dim = mergedDimensions.get(i);
      final ColumnPartWriter dimWriter = dimWriters.get(i);
      final GenericIndexedWriter<String> dictionaryWriter = dictionaryWriters == null ? null : dictionaryWriters.get(i);
      final ColumnPartSerde fst = dictionaryFSTs == null ? null : dictionaryFSTs.get(dim);
      final ColumnPartWriter<ImmutableBitmap> bitmapIndexWriter =
          bitmapIndexWriters == null ? null : bitmapIndexWriters.get(i);
      final ColumnPartWriter<ImmutableRTree> spatialIndexWriter =
          spatialIndexWriters == null ? null : spatialIndexWriters.get(i);
      final CompressionStrategy compression =
          dimWriter instanceof Compressed ? ((Compressed) dimWriter).appliedCompression() : null;

      dimWriter.close();
      bitmapIndexWriter.close();
      Closeables.close(spatialIndexWriter, false);

      final boolean hasMultiValue = dimCapabilities != null && dimCapabilities.get(i).hasMultipleValues();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      builder.setValueType(ValueDesc.STRING);
      builder.setHasMultipleValues(hasMultiValue);
      final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = DictionaryEncodedColumnPartSerde
          .serializerBuilder()
          .withDictionary(dictionaryWriter)
          .withValue(dimWriter, hasMultiValue, compression != null)
          .withBitmapIndex(bitmapIndexWriter)
          .withSpatialIndex(spatialIndexWriter);

      builder.addSerde(partBuilder.build(bitmapSerdeFactory));
      if (fst != null) {
        builder.addSerde(fst);
      }

      makeColumn(v9Smoosher, dim, builder.build(includeStats));
      log.info("Completed dimension column[%s] in %,d millis.", dim, System.currentTimeMillis() - dimStartTime);
    }
    log.info("Completed dimension columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void makeMetricsColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final List<String> mergedMetrics,
      final Map<String, ValueDesc> metricTypeNames,
      final List<MetricColumnSerializer> metWriters,
      final boolean includeStats
  ) throws IOException
  {
    final String section = "make metric columns";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < mergedMetrics.size(); ++i) {
      String metric = mergedMetrics.get(i);
      long metricStartTime = System.currentTimeMillis();
      MetricColumnSerializer writer = metWriters.get(i);
      writer.close();

      ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      writer.buildDescriptor(metricTypeNames.get(metric), builder);

      makeColumn(v9Smoosher, metric, builder.build(includeStats));
      log.info("Completed metric column[%s] in %,d millis.", metric, System.currentTimeMillis() - metricStartTime);
    }
    log.info("Completed metric columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }


  private void makeTimeColumn(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final String columnName,
      final GenericColumnSerializer timeWriter,
      final boolean includeStats
  ) throws IOException
  {
    final String section = "make time column";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    timeWriter.close();

    ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    timeWriter.buildDescriptor(ValueDesc.LONG, builder);

    makeColumn(v9Smoosher, columnName, builder.build(includeStats));
    log.info("Completed time column in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void makeColumn(
      final FileSmoosher v9Smoosher,
      final String columnName,
      final ColumnDescriptor serdeficator
  ) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String descriptor = mapper.writeValueAsString(serdeficator);
    log.debug("> %s : %s", columnName, descriptor);
    SerializerUtils.writeString(baos, descriptor);
    byte[] specBytes = baos.toByteArray();

    final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
        columnName, serdeficator.numBytes() + specBytes.length
    );
    try {
      channel.write(ByteBuffer.wrap(specBytes));
      serdeficator.write(channel);
    }
    finally {
      channel.close();
    }
  }

  private void makeInvertedIndexes(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final IndexSpec indexSpec,
      final File v9OutDir,
      final int[][] rowNumConversions,
      final ArrayList<MutableBitmap> nullRowsList,
      final ArrayList<Indexed.Closeable<String>> dimValues,
      final ArrayList<ColumnPartWriter<ImmutableBitmap>> bitmapIndexWriters,
      final ArrayList<ColumnPartWriter<ImmutableRTree>> spatialIndexWriters,
      final ArrayList<Map<String, IntBuffer>> dimConversions
  ) throws IOException
  {
    final String section = "build bitmap index";
    progress.startSection(section);

    final long startTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      String dimension = mergedDimensions.get(dimIndex);
      long dimStartTime = System.currentTimeMillis();

      final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory(true);

      ColumnPartWriter<ImmutableRTree> spatialIndexWriter = spatialIndexWriters.get(dimIndex);
      RTree tree = null;
      if (spatialIndexWriter != null) {
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
      }

      IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimension);
      ImmutableBitmap nullRowBitmap = bitmapFactory.makeImmutableBitmap(nullRowsList.get(dimIndex));

      //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
      final Indexed.Closeable<String> dimVals = dimValues.get(dimIndex);
      for (int dictId = 0; dictId < dimVals.size(); dictId++) {
        progress.progress();
        final List<IntIterator> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
        for (int j = 0; j < adapters.size(); ++j) {
          final int seekedDictId = dictIdSeeker[j].seek(dictId);
          if (seekedDictId != IndexSeeker.NOT_EXIST) {
            ImmutableBitmap bitmap = adapters.get(j).getBitmap(dimension, seekedDictId);
            if (bitmap == null) {
              continue;
            }
            if (rowNumConversions != null) {
              convertedInverteds.add(IntIterators.map(bitmap.iterator(), rowNumConversions[j]));
            } else {
              convertedInverteds.add(bitmap.iterator());
            }
          }
        }

        MutableBitmap bitmap = toBitmap(convertedInverteds, bitmapFactory.makeEmptyMutableBitmap());
        ImmutableBitmap bitmapToWrite = bitmapFactory.makeImmutableBitmap(bitmap);
        if (dictId == 0 && dimVals.get(dictId) == null) {
          bitmapToWrite = nullRowBitmap.union(bitmapToWrite);
        }
        bitmapIndexWriters.get(dimIndex).add(bitmapToWrite);

        if (spatialIndexWriter != null) {
          String dimVal = dimVals.get(dictId);
          if (dimVal != null) {
            List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
            float[] coords = new float[stringCoords.size()];
            for (int j = 0; j < coords.length; j++) {
              coords[j] = Float.valueOf(stringCoords.get(j));
            }
            tree.insert(coords, bitmap);
          }
        }
      }
      if (spatialIndexWriter != null) {
        spatialIndexWriter.add(ImmutableRTree.newImmutableFromMutable(tree));
      }
      dimVals.close();

      log.info(
          "Completed dim[%s] bitmap index with cardinality[%,d] in %,d millis.",
          dimension,
          dimVals.size(),
          System.currentTimeMillis() - dimStartTime
      );
    }
    log.info("Completed bitmap index in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private static final int MAX_GROUP = 32;
  private static final double CUMULATIVE_THRESHOLD = 16384;

  private ArrayList<ColumnPartWriter<ImmutableBitmap>> setupBitmapIndexWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final Map<String, Integer> dimCardinalities,
      final IndexSpec indexSpec
  ) throws IOException
  {
    BitmapSerdeFactory serdeFactory = indexSpec.getBitmapSerdeFactory();
    ArrayList<ColumnPartWriter<ImmutableBitmap>> writers = Lists.newArrayListWithCapacity(mergedDimensions.size());
    BitmapFactory bitmapFactory = serdeFactory.getBitmapFactory();
    for (String dimension : mergedDimensions) {
      ColumnPartWriter<ImmutableBitmap> writer = GenericIndexedWriter.v2(
          ioPeon, String.format("%s.inverted", dimension), serdeFactory.getObjectStrategy()
      );
      Integer cardinality = dimCardinalities.get(dimension);
      if (cardinality != null && cardinality > CUMULATIVE_THRESHOLD) {
        int group = Math.min(MAX_GROUP, (int) Math.ceil(cardinality / CUMULATIVE_THRESHOLD));
        int threshold = cardinality / group + group;
        writer = new CumulativeBitmapWriter(
            ioPeon, String.format("%s.inverted.cumulative", dimension), writer, serdeFactory, threshold
        );
      }
      writer.open();
      writers.add(writer);
    }
    return writers;
  }

  private ArrayList<ColumnPartWriter<ImmutableRTree>> setupSpatialIndexWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final IndexSpec indexSpec,
      final List<ColumnCapabilities> dimCapabilities
  ) throws IOException
  {
    ArrayList<ColumnPartWriter<ImmutableRTree>> writers = Lists.newArrayListWithCapacity(mergedDimensions.size());
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      if (dimCapabilities.get(dimIndex).hasSpatialIndexes()) {
        BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
        ColumnPartWriter<ImmutableRTree> writer = new ByteBufferWriter<>(
            ioPeon,
            String.format("%s.spatial", mergedDimensions.get(dimIndex)),
            new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory)
        );
        writer.open();
        writers.add(writer);
      } else {
        writers.add(null);
      }
    }
    return writers;
  }

  private void writeRowValues(
      final ProgressIndicator progress,
      final Iterator<Rowboat> theRows,
      final LongColumnSerializer timeWriter,
      final ArrayList<ColumnPartWriter> dimWriters,
      final ArrayList<MetricColumnSerializer> metWriters,
      final boolean[] dimensionSkipFlag,
      final ArrayList<MutableBitmap> nullRowsList,
      final boolean[] dimHasNullFlags
  ) throws IOException
  {
    final String section = "walking through rows";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    int rowNum = 0;
    long time = System.currentTimeMillis();
    while (theRows.hasNext()) {
      Rowboat theRow = theRows.next();
      progress.progress();
      timeWriter.serialize(rowNum, theRow.getTimestamp());

      final Object[] metrics = theRow.getMetrics();
      for (int i = 0; i < metrics.length; ++i) {
        metWriters.get(i).serialize(rowNum, metrics[i]);
      }

      final int[][] dims = theRow.getDims();
      for (int i = 0; i < dims.length; ++i) {
        if (dimensionSkipFlag[i]) {
          continue;
        }
        if (dims[i] == null || dims[i].length == 0) {
          nullRowsList.get(i).add(rowNum);
        } else if (dimHasNullFlags[i] && dims[i].length == 1 && dims[i][0] == 0) {
          // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
          // that matches the null/empty str's dictionary ID should also be added to nullRowsList.
          nullRowsList.get(i).add(rowNum);
        }
        dimWriters.get(i).add(dims[i]);
      }

      if ((++rowNum % 500_000) == 0) {
        log.info("..walked 500,000 rows.. total %,d rows in %,d millis.", rowNum, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    log.info("Completed walking through of %,d rows in %,d millis.", rowNum, System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void writeCubeRowValues(
      final ProgressIndicator progress,
      final Iterator<Rowboat> rows,
      final LongColumnSerializer timeWriter,
      final List<ColumnPartWriter> dimWriters,
      final List<MetricColumnSerializer> metWriters,
      final MutableBitmap[][] bitmaps
  ) throws IOException
  {
    final String section = "walking through rows";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    int rowNum = 0;
    long time = System.currentTimeMillis();
    while (rows.hasNext()) {
      Rowboat row = rows.next();
      progress.progress();
      timeWriter.serialize(rowNum, row.getTimestamp());

      // cube dims..
      final int[] dims = row.getDims()[0];
      for (int i = 0; i < dims.length; ++i) {
        dimWriters.get(i).add(dims[i]);
        bitmaps[i][dims[i]].add(rowNum);
      }
      final Object[] metrics = row.getMetrics();
      for (int i = 0; i < metrics.length; ++i) {
        metWriters.get(i).serialize(rowNum, metrics[i]);
      }

      if ((++rowNum % 500_000) == 0) {
        log.info("..walked 500,000 rows.. total %,d rows in %,d millis.", rowNum, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    log.info("Completed walking through %,d rows in %,d millis.", rowNum, System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private LongColumnSerializer setupTimeWriter(
      final IOPeon ioPeon,
      final String fileNameBase,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final SecondaryIndexingSpec indexing = indexSpec.getSecondaryIndexingSpec(Column.TIME_COLUMN_NAME);
    final BitmapSerdeFactory serdeFactory = indexSpec.getBitmapSerdeFactory();
    final LongColumnSerializer timeWriter = LongColumnSerializer.create(
        fileNameBase, indexSpec.getMetricCompressionStrategy(), serdeFactory, indexing, false
    );
    // we will close this writer after we added all the timestamps
    timeWriter.open(ioPeon);
    return timeWriter;
  }

  private ArrayList<MetricColumnSerializer> setupMetricsWriters(
      final IOPeon ioPeon,
      final List<String> mergedMetrics,
      final Map<String, ValueDesc> metricTypeNames,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final ArrayList<MetricColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());
    for (String metric : mergedMetrics) {
      final ValueDesc type = metricTypeNames.get(metric);
      metWriters.add(setupMetricsWriter(ioPeon, metric, type, indexSpec));
    }
    return metWriters;
  }

  private MetricColumnSerializer setupMetricsWriter(IOPeon ioPeon, String metric, ValueDesc type, IndexSpec indexSpec)
      throws IOException
  {
    final BitmapSerdeFactory serdeFactory = indexSpec.getBitmapSerdeFactory();
    final SecondaryIndexingSpec secondary = indexSpec.getSecondaryIndexingSpec(metric);
    final boolean allowNullForNumbers = indexSpec.isAllowNullForNumbers();
    final CompressionStrategy compression = indexSpec.getCompressionStrategy(metric, null);
    final CompressionStrategy metCompression = Optional.ofNullable(compression).orElse(indexSpec.getMetricCompressionStrategy());

    final MetricColumnSerializer writer;
    switch (type.type()) {
      case BOOLEAN:
        writer = BooleanColumnSerializer.create(serdeFactory);
        break;
      case LONG:
        writer = LongColumnSerializer.create(metric, metCompression, serdeFactory, secondary, allowNullForNumbers);
        break;
      case FLOAT:
        writer = FloatColumnSerializer.create(metric, metCompression, serdeFactory, secondary, allowNullForNumbers);
        break;
      case DOUBLE:
        writer = DoubleColumnSerializer.create(metric, metCompression, serdeFactory, secondary, allowNullForNumbers);
        break;
      case STRING:
        writer = ComplexColumnSerializer.create(metric, StringMetricSerde.INSTANCE, secondary, compression);
        break;
      case COMPLEX:
        final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type);
        if (serde == null) {
          throw new ISE("Unknown type[%s]", type);
        }
        // todo : compression (ComplexMetricSerde is not implementing this except StringMetricSerde)
        writer = ComplexColumnSerializer.create(metric, serde, secondary, compression);
        break;
      default:
        throw new ISE("Unknown type[%s]", type);
    }
    // we will close these writers in another method after we added all the metrics
    writer.open(ioPeon);
    return writer;
  }

  // heuristic
  private static final int SKIP_COMPRESSION_THRESHOLD = Short.MAX_VALUE << 2;

  private ArrayList<ColumnPartWriter> setupDimensionWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final List<ColumnCapabilities> dimCapabilities,
      final Map<String, Integer> dimCardinalities,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final ArrayList<ColumnPartWriter> dimWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    final CompressionStrategy dimCompression = indexSpec.getDimensionCompressionStrategy();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      String dim = mergedDimensions.get(dimIndex);
      int cardinality = dimCardinalities.get(dim);
      ColumnCapabilities capabilities = dimCapabilities == null ? null : dimCapabilities.get(dimIndex);
      String filenameBase = String.format("%s.forward_dim", dim);
      CompressionStrategy compression = cardinality < SKIP_COMPRESSION_THRESHOLD ? dimCompression : null;
      ColumnPartWriter writer;
      if (capabilities != null && capabilities.hasMultipleValues()) {
        writer = compression != null
                 ? CompressedVSizeIntsV3Writer.create(ioPeon, filenameBase, cardinality, compression)
                 : new VSizeIntsWriter(ioPeon, filenameBase, cardinality);
      } else {
        writer = compression != null
                 ? CompressedVSizeIntWriter.create(ioPeon, filenameBase, cardinality, compression)
                 : new VSizeIntWriter(ioPeon, filenameBase, cardinality);
      }
      writer.open();
      // we will close these writers in another method after we added all the values
      dimWriters.add(writer);
    }
    return dimWriters;
  }

  private ArrayList<GenericIndexedWriter<String>> setupDictionaryWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions
  )
      throws IOException
  {
    ArrayList<GenericIndexedWriter<String>> dimValueWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    for (String dimension : mergedDimensions) {
      final GenericIndexedWriter<String> writer = GenericIndexedWriter.forDictionaryV2(
          ioPeon, String.format("%s.dim_values", dimension)
      );
      writer.open();
      dimValueWriters.add(writer);
    }
    return dimValueWriters;
  }

  private void writeDimValueAndSetupDimConversion(
      final List<IndexableAdapter> indexes,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<GenericIndexedWriter<String>> dictionaryWriters,
      final Map<String, ColumnPartSerde> dictionaryFSTs,
      final Map<String, Integer> dimensionCardinalities,
      final boolean[] dimensionSkipFlag,
      final List<Map<String, IntBuffer>> dimConversions,
      final boolean[] convertMissingDimsFlags,
      final boolean[] dimHasNullFlags,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "build dictionary";
    progress.startSection(section);

    final long startTime = System.currentTimeMillis();

    for (int i = 0; i < indexes.size(); ++i) {
      dimConversions.add(Maps.<String, IntBuffer>newHashMap());
    }

    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      long dimStartTime = System.currentTimeMillis();
      String dimension = mergedDimensions.get(dimIndex);
      boolean dimHasNull = false;
      boolean dimHasValues = false;
      boolean dimAbsentFromSomeIndex = false;

      int numMergeIndex = 0;
      Indexed<String> dimValueLookup = null;
      Indexed<String>[] dimValueLookups = new Indexed[indexes.size() + 1];
      for (int i = 0; i < indexes.size(); i++) {
        Indexed<String> dimValues = indexes.get(i).getDimValueLookup(dimension);
        if (!isNullColumn(dimValues)) {
          dimHasValues = true;
          dimHasNull |= dimValues.indexOf(null) >= 0;
          dimValueLookups[i] = dimValueLookup = dimValues;
          numMergeIndex++;
        } else {
          dimAbsentFromSomeIndex = true;
        }
      }

      boolean convertMissingDims = dimHasValues && dimAbsentFromSomeIndex;
      convertMissingDimsFlags[dimIndex] = convertMissingDims;

      /*
       * Ensure the empty str is always in the dictionary if the dimension was missing from one index but
       * has non-null values in another index.
       * This is done so that MMappedIndexRowIterable can convert null columns to empty strings
       * later on, to allow rows from indexes without a particular dimension to merge correctly with
       * rows from indexes with null/empty str values for that dimension.
       */
      if (convertMissingDims && !dimHasNull) {
        dimHasNull = true;
        dimValueLookups[indexes.size()] = dimValueLookup = EMPTY_STR_DIM_VAL;
        numMergeIndex++;
      }

      final DictionaryPartBuilder builder = indexSpec.getFSTBuilder(dimension, mapper);
      final GenericIndexedWriter<String> writer = dictionaryWriters.get(dimIndex);

      int cardinality = 0;
      if (numMergeIndex > 1) {
        final DictionaryMergeIterator iterator = new DictionaryMergeIterator(dimValueLookups, true);

        for (int i = 0; iterator.hasNext(); i++) {
          String value = iterator.next();
          writer.add(value);
          if (builder != null) {
            builder.addEntry(Strings.nullToEmpty(value), i);
          }
        }

        for (int i = 0; i < indexes.size(); i++) {
          if (dimValueLookups[i] != null && iterator.needConversion(i)) {
            dimConversions.get(i).put(dimension, iterator.conversions[i]);
          }
        }
        cardinality = iterator.counter;
      } else if (numMergeIndex == 1) {
        int i = 0;
        for (String value : dimValueLookup) {
          writer.add(value);
          if (builder != null) {
            builder.addEntry(Strings.nullToEmpty(value), i++);
          }
        }
        cardinality = dimValueLookup.size();
      }
      ColumnPartSerde fst = builder == null ? null : builder.done(cardinality);
      if (fst != null) {
        dictionaryFSTs.put(dimension, fst);
      }

      // Mark if this dim has the null/empty str value in its dictionary, used for determining nullRowsList later.
      dimHasNullFlags[dimIndex] = dimHasNull;

      log.info(
          "Completed dim[%s] dictionary with cardinality[%,d] in %,d millis.",
          dimension,
          cardinality,
          System.currentTimeMillis() - dimStartTime
      );
      dimensionCardinalities.put(dimension, cardinality);
      writer.close();

      if (cardinality == 0) {
        log.info(String.format("Skipping [%s], it is empty!", dimension));
        dimensionSkipFlag[dimIndex] = true;
      }
    }
    log.info("Completed dictionaries in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void mergeCapabilities(
      final List<IndexableAdapter> adapters,
      final List<String> mergedDimensions,
      final Map<String, ValueDesc> metricTypeNames,
      final List<ColumnCapabilities> dimCapabilities
  )
  {
    final Map<String, ColumnCapabilities> capabilitiesMap = Maps.newHashMap();
    for (IndexableAdapter adapter : adapters) {
      for (String dimension : adapter.getDimensionNames()) {
        ColumnCapabilities previous = capabilitiesMap.get(dimension);
        ColumnCapabilities merged;
        if (previous == null) {
          merged = adapter.getCapabilities(dimension);
        } else {
          merged = previous.merge(adapter.getCapabilities(dimension));
        }
        capabilitiesMap.put(dimension, Preconditions.checkNotNull(merged));
      }
      for (String metric : adapter.getMetricNames()) {
        ColumnCapabilities mergedCapabilities = capabilitiesMap.get(metric);
        ColumnCapabilities capabilities = adapter.getCapabilities(metric);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilities();
        }
        capabilitiesMap.put(metric, mergedCapabilities.merge(capabilities));
        metricTypeNames.put(metric, adapter.getMetricType(metric));
      }
    }
    for (String dim : mergedDimensions) {
      dimCapabilities.add(capabilitiesMap.get(dim));
    }
  }
}
