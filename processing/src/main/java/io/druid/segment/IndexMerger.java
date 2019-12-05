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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.SerializerUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIndexedWriter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.serde.ComplexMetricColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class IndexMerger
{
  private static final Logger log = new Logger(IndexMerger.class);

  protected static final ListIndexed<String> EMPTY_STR_DIM_VAL = new ListIndexed<>(Arrays.asList(""), String.class);
  protected static final Splitter SPLITTER = Splitter.on(",");

  protected final ObjectMapper mapper;
  protected final IndexIO indexIO;

  @Inject
  public IndexMerger(
      ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");

  }

  public IndexIO getIndexIO()
  {
    return indexIO;
  }

  public File persist(
      final IncrementalIndex index,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, index.getInterval(), outDir, indexSpec);
  }

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @return the index output directory
   *
   * @throws java.io.IOException if an IO error occurs persisting the index
   */
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, indexSpec, new BaseProgressIndicator());
  }

  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    if (index.isEmpty()) {
      throw new IAE("Trying to persist an empty index!");
    }

    final long firstTimestamp = index.getMinTime().getMillis();
    final long lastTimestamp = index.getMaxTime().getMillis();
    if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp))) {
      throw new IAE(
          "interval[%s] does not encapsulate the full range of timestamps[%s, %s]",
          dataInterval,
          new DateTime(firstTimestamp),
          new DateTime(lastTimestamp)
      );
    }

    if (!outDir.exists() && !outDir.mkdirs()) {
      throw new ISE("Cannot create directory [%s]", outDir);
    }
    if (!outDir.isDirectory()) {
      throw new ISE("Can only persist to directories, [%s] wasn't a directory", outDir);
    }

    log.info("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    return merge(
        Arrays.<IndexableAdapter>asList(
            new IncrementalIndexAdapter(
                dataInterval,
                index,
                indexSpec.getBitmapSerdeFactory().getBitmapFactory()
            )
        ),
        index.isRollup(),
        index.getMetricAggs(),
        outDir,
        indexSpec,
        progress
    );
  }

  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator());
  }

  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    // We are materializing the list for performance reasons. Lists.transform
    // only creates a "view" of the original list, meaning the function gets
    // applied every time you access an element.
    List<IndexableAdapter> indexAdapteres = Lists.newArrayList(
        Iterables.transform(
            indexes,
            new Function<QueryableIndex, IndexableAdapter>()
            {
              @Override
              public IndexableAdapter apply(final QueryableIndex input)
              {
                return new QueryableIndexIndexableAdapter(input);
              }
            }
        )
    );
    return merge(
        indexAdapteres,
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        progress
    );
  }

  public File mergeQueryableIndexAndClose(
      List<QueryableIndex> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    try {
      return mergeQueryableIndex(indexes, rollup, metricAggs, outDir, indexSpec, progress);
    }
    finally {
      for (QueryableIndex index : indexes) {
        Closeables.close(index, true);
      }
    }
  }

  public File merge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return merge(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator());
  }

  private static List<String> getLexicographicMergedDimensions(List<IndexableAdapter> indexes)
  {
    return mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
  }

  private static List<String> getLongestSharedDimOrder(List<IndexableAdapter> indexes)
  {
    int maxSize = 0;
    Iterable<String> orderingCandidate = null;
    for (IndexableAdapter index : indexes) {
      int iterSize = index.getDimensionNames().size();
      if (iterSize > maxSize) {
        maxSize = iterSize;
        orderingCandidate = index.getDimensionNames();
      }
    }

    if (orderingCandidate == null) {
      return null;
    }

    for (IndexableAdapter index : indexes) {
      Iterator<String> candidateIter = orderingCandidate.iterator();
      for (String matchDim : index.getDimensionNames()) {
        boolean matched = false;
        while (candidateIter.hasNext()) {
          String nextDim = candidateIter.next();
          if (matchDim.equals(nextDim)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return null;
        }
      }
    }
    return ImmutableList.copyOf(orderingCandidate);
  }

  public static List<String> getMergedDimensions(List<IndexableAdapter> indexes)
  {
    if (indexes.size() == 0) {
      return ImmutableList.of();
    }
    List<String> commonDimOrder = getLongestSharedDimOrder(indexes);
    if (commonDimOrder == null) {
      log.warn("Indexes have incompatible dimension orders, using lexicographic order.");
      return getLexicographicMergedDimensions(indexes);
    } else {
      return commonDimOrder;
    }
  }

  private File merge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final List<String> mergedDimensions = getMergedDimensions(indexes);

    final List<String> mergedMetrics = Lists.transform(
        mergeIndexed(
            Lists.newArrayList(
                FunctionalIterable
                    .create(indexes)
                    .transform(
                        new Function<IndexableAdapter, Iterable<String>>()
                        {
                          @Override
                          public Iterable<String> apply(@Nullable IndexableAdapter input)
                          {
                            return input.getMetricNames();
                          }
                        }
                    )
            )
        ),
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input;
          }
        }
    );

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (int i = 0; i < metricAggs.length; i++) {
      AggregatorFactory metricAgg = metricAggs[i];
      int metricIndex = mergedMetrics.indexOf(metricAgg.getName());
      /*
        If metricIndex is negative, one of the metricAggs was not present in the union of metrics from the indices
        we are merging
       */
      if (metricIndex > -1) {
        sortedMetricAggs[metricIndex] = metricAgg;
      }
    }

    /*
      If there is nothing at sortedMetricAggs[i], then we did not have a metricAgg whose name matched the name
      of the ith element of mergedMetrics. I.e. There was a metric in the indices to merge that we did not ask for.
     */
    for (int i = 0; i < sortedMetricAggs.length; i++) {
      if (sortedMetricAggs[i] == null) {
        throw new IAE("Indices to merge contained metric[%s], but requested metrics did not", mergedMetrics.get(i));
      }
    }

    for (int i = 0; i < mergedMetrics.size(); i++) {
      if (!sortedMetricAggs[i].getName().equals(mergedMetrics.get(i))) {
        throw new IAE(
            "Metric mismatch, index[%d] [%s] != [%s]",
            i,
            sortedMetricAggs[i].getName(),
            mergedMetrics.get(i)
        );
      }
    }
    final int[][] rowNumConversions;
    final Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>> rowMergerFn;
    if (indexes.size() == 1) {
      rowNumConversions = null;
      rowMergerFn = new Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>>()
      {
        @Override
        public Iterator<Rowboat> apply(ArrayList<Iterator<Rowboat>> input)
        {
          return input.get(0);
        }
      };
    } else {
      rowNumConversions = new int[indexes.size()][];
      for (int i = 0; i < rowNumConversions.length; i++) {
        rowNumConversions[i] = new int[indexes.get(i).getNumRows()];
      }
      final Ordering<Rowboat> ordering;
      final BinaryFn.Identical<Rowboat> merger;
      if (rollup) {
        ordering = Ordering.<Rowboat>natural();
        merger = new RowboatMergeFunction(AggregatorFactory.toCombiner(sortedMetricAggs));
      } else {
        ordering = Rowboat.TIME_ONLY_COMPARATOR;
        merger = null;
      }
      rowMergerFn = new Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>>()
      {
        @Override
        public Iterator<Rowboat> apply(ArrayList<Iterator<Rowboat>> boats)
        {
          return new RowNumIterator(boats, rowNumConversions, merger, ordering);
        }
      };
    }

    return makeIndexFiles(
        indexes,
        sortedMetricAggs,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        rowNumConversions,
        indexSpec
    );
  }

  // Faster than IndexMaker
  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator());
  }

  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec, final ProgressIndicator progress)
      throws IOException
  {
    try (QueryableIndex index = indexIO.loadIndex(inDir)) {
      final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
      return makeIndexFiles(
          ImmutableList.of(adapter),
          null,
          outDir,
          progress,
          Lists.newArrayList(adapter.getDimensionNames()),
          Lists.newArrayList(adapter.getMetricNames()),
          new Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>>()
          {
            @Override
            public Iterator<Rowboat> apply(ArrayList<Iterator<Rowboat>> input)
            {
              return input.get(0);
            }
          },
          null,
          indexSpec
      );
    }
  }


  public File append(List<IndexableAdapter> indexes, AggregatorFactory[] aggregators, File outDir, IndexSpec indexSpec)
      throws IOException
  {
    return append(indexes, aggregators, outDir, indexSpec, new BaseProgressIndicator());
  }

  public File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    final List<String> mergedDimensions = getMergedDimensions(indexes);

    final List<String> mergedMetrics = mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getMetricNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(String input)
                      {
                        return input;
                      }
                    }
                );
              }
            }
        )
    );

    final int[][] rowNumConversions = new int[indexes.size()][];
    for (int i = 0; i < rowNumConversions.length; i++) {
      rowNumConversions[i] = new int[indexes.get(i).getNumRows()];
    }

    // no rollup
    final Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>> rowMergerFn =
        new Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>>()
        {
          @Override
          public Iterator<Rowboat> apply(ArrayList<Iterator<Rowboat>> boats)
          {
            return new RowNumIterator(boats, rowNumConversions, null, Ordering.natural());
          }
        };

    return makeIndexFiles(
        indexes,
        aggregators,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        rowNumConversions,
        indexSpec
    );
  }

  protected File makeIndexFiles(
      final List<IndexableAdapter> indexes,
      final AggregatorFactory[] metricAggs,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>> rowMergerFn,
      final int[][] rowNumConversions,
      final IndexSpec indexSpec
  ) throws IOException
  {
    List<Metadata> metadataList = Lists.transform(
        indexes,
        new Function<IndexableAdapter, Metadata>()
        {
          @Nullable
          @Override
          public Metadata apply(IndexableAdapter input)
          {
            return input.getMetadata();
          }
        }
    );

    AggregatorFactory[] combiningMetricAggs = null;
    if (metricAggs != null) {
      combiningMetricAggs = new AggregatorFactory[metricAggs.length];
      for (int i = 0; i < metricAggs.length; i++) {
        combiningMetricAggs[i] = metricAggs[i].getCombiningFactory();
      }
    }
    Metadata segmentMetadata = Metadata.merge(metadataList, combiningMetricAggs);

    final Map<String, ValueDesc> metricTypeNames = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
    final Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();

    for (IndexableAdapter adapter : indexes) {
      for (String dimension : adapter.getDimensionNames()) {
        ColumnCapabilities mergedCapabilities = columnCapabilities.get(dimension);
        ColumnCapabilities capabilities = adapter.getCapabilities(dimension);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilities();
          mergedCapabilities.setType(ValueType.STRING);
        }
        columnCapabilities.put(dimension, mergedCapabilities.merge(capabilities));
      }
      for (String metric : adapter.getMetricNames()) {
        ColumnCapabilities mergedCapabilities = columnCapabilities.get(metric);
        ColumnCapabilities capabilities = adapter.getCapabilities(metric);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilities();
        }
        columnCapabilities.put(metric, mergedCapabilities.merge(capabilities));
        metricTypeNames.put(metric, adapter.getMetricType(metric));
      }
    }

    Closer closer = Closer.create();
    final Interval dataInterval;
    final File v8OutDir = new File(outDir, "v8-tmp");
    v8OutDir.mkdirs();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        FileUtils.deleteDirectory(v8OutDir);
      }
    });
    final IOPeon ioPeon = new TmpFileIOPeon();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        ioPeon.cleanup();
      }
    });
    try {
      /*************  Main index.drd file **************/
      progress.progress();
      long startTime = System.currentTimeMillis();
      File indexFile = new File(v8OutDir, "index.drd");

      try (FileOutputStream fileOutputStream = new FileOutputStream(indexFile);
           FileChannel channel = fileOutputStream.getChannel()) {
        channel.write(ByteBuffer.wrap(new byte[]{IndexIO.V8_VERSION}));

        GenericIndexed.fromIterable(mergedDimensions, ObjectStrategy.STRING_STRATEGY).writeToChannel(channel);
        GenericIndexed.fromIterable(mergedMetrics, ObjectStrategy.STRING_STRATEGY).writeToChannel(channel);

        DateTime minTime = new DateTime(JodaUtils.MAX_INSTANT);
        DateTime maxTime = new DateTime(JodaUtils.MIN_INSTANT);

        for (IndexableAdapter index : indexes) {
          minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
          maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
        }

        dataInterval = new Interval(minTime, maxTime);
        SerializerUtils.writeString(channel, String.format("%s/%s", minTime, maxTime));
        SerializerUtils.writeString(channel, mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory()));
      }
      IndexIO.checkFileSize(indexFile);
      log.info("outDir[%s] completed index.drd in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

      /************* Setup Dim Conversions **************/
      progress.progress();
      startTime = System.currentTimeMillis();

      final ArrayList<FileOutputSupplier> dimOuts = Lists.newArrayListWithCapacity(mergedDimensions.size());
      final Map<String, Integer> dimensionCardinalities = Maps.newHashMap();
      final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(indexes.size());
      final boolean[] convertMissingDimsFlags = new boolean[mergedDimensions.size()];
      final ArrayList<MutableBitmap> nullRowsList = Lists.newArrayListWithCapacity(mergedDimensions.size());
      final boolean[] dimHasNullFlags = new boolean[mergedDimensions.size()];

      for (int i = 0; i < indexes.size(); ++i) {
        dimConversions.add(Maps.<String, IntBuffer>newHashMap());
      }

      for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
        final String dimension = mergedDimensions.get(dimIndex);
        nullRowsList.add(indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap());

        ColumnPartWriter<String> writer = GenericIndexedWriter.forDictionary(ioPeon, dimension);
        writer.open();

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

        int cardinality = 0;
        if (numMergeIndex > 1) {
          DictionaryMergeIterator iterator = new DictionaryMergeIterator(dimValueLookups, true);

          while (iterator.hasNext()) {
            writer.add(iterator.next());
          }

          for (int i = 0; i < indexes.size(); i++) {
            if (dimValueLookups[i] != null && iterator.needConversion(i)) {
              dimConversions.get(i).put(dimension, iterator.conversions[i]);
            }
          }
          cardinality = iterator.counter;
        } else if (numMergeIndex == 1) {
          for (String value : dimValueLookup) {
            writer.add(value);
          }
          cardinality = dimValueLookup.size();
        }

        dimensionCardinalities.put(dimension, cardinality);

        // Mark if this dim has the null/empty str value in its dictionary, used for determining nullRowsList later.
        dimHasNullFlags[dimIndex] = dimHasNull;

        FileOutputSupplier dimOut = new FileOutputSupplier(IndexIO.makeDimFile(v8OutDir, dimension), true);
        dimOuts.add(dimOut);

        writer.close();
        try (WritableByteChannel channel = Channels.newChannel(dimOut.getOutput())) {
          SerializerUtils.writeString(channel, dimension);
          writer.writeToChannel(channel);
        }

        ioPeon.cleanup();
      }
      log.info("outDir[%s] completed dim conversions in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

      /************* Walk through data sets and merge them *************/
      progress.progress();
      startTime = System.currentTimeMillis();

      Iterator<Rowboat> theRows = makeRowIterable(
          indexes,
          mergedDimensions,
          mergedMetrics,
          dimConversions,
          convertMissingDimsFlags,
          rowMergerFn
      );

      CompressedLongsSupplierSerializer timeWriter = CompressedLongsSupplierSerializer.create(
          ioPeon, "little_end_time", IndexIO.BYTE_ORDER, CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY
      );

      timeWriter.open();

      ArrayList<VSizeIndexedWriter> forwardDimWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
      for (String dimension : mergedDimensions) {
        VSizeIndexedWriter writer = new VSizeIndexedWriter(ioPeon, dimension, dimensionCardinalities.get(dimension));
        writer.open();
        forwardDimWriters.add(writer);
      }

      ArrayList<MetricColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());
      for (String metric : mergedMetrics) {
        ValueDesc type = metricTypeNames.get(metric);
        switch (type.type()) {
          case LONG:
            metWriters.add(new LongMetricColumnSerializer(metric, v8OutDir, ioPeon));
            break;
          case FLOAT:
            metWriters.add(new FloatMetricColumnSerializer(metric, v8OutDir, ioPeon));
            break;
          case DOUBLE:
            metWriters.add(new DoubleMetricColumnSerializer(metric, v8OutDir, ioPeon));
            break;
          case COMPLEX:
            final String typeName = type.typeName();
            ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

            if (serde == null) {
              throw new ISE("Unknown type[%s]", typeName);
            }

            metWriters.add(new ComplexMetricColumnSerializer(metric, v8OutDir, ioPeon, serde));
            break;
          default:
            throw new ISE("Unknown type[%s]", type);
        }
      }

      for (MetricColumnSerializer metWriter : metWriters) {
        metWriter.open();
      }

      long time = System.currentTimeMillis();

      int rowCount = 0;
      while (theRows.hasNext()) {
        Rowboat theRow = theRows.next();
        progress.progress();
        timeWriter.add(theRow.getTimestamp());

        final Object[] metrics = theRow.getMetrics();
        for (int i = 0; i < metrics.length; ++i) {
          metWriters.get(i).serialize(rowCount, metrics[i]);
        }

        final int[][] dims = theRow.getDims();
        for (int i = 0; i < dims.length; ++i) {
          List<Integer> listToWrite = (i >= dims.length || dims[i] == null)
                                      ? null
                                      : Ints.asList(dims[i]);
          forwardDimWriters.get(i).write(listToWrite);
          if (listToWrite == null || listToWrite.isEmpty()) {
            // empty row; add to the nullRows bitmap
            nullRowsList.get(i).add(rowCount);
          } else if (dimHasNullFlags[i] && listToWrite.size() == 1 && listToWrite.get(0) == 0) {
            // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
            // that matches the null/empty str's dictionary ID should also be added to nullRowsList.
            nullRowsList.get(i).add(rowCount);
          }
        }

        if (++rowCount % 500000 == 0) {
          log.info(
              "outDir[%s] walked 500,000/%,d rows in %,d millis.", v8OutDir, rowCount, System.currentTimeMillis() - time
          );
          time = System.currentTimeMillis();
        }
      }

      File timeFile = IndexIO.makeTimeFile(v8OutDir, IndexIO.BYTE_ORDER);
      timeFile.delete();
      timeWriter.close();
      try (WritableByteChannel channel = new FileOutputStream(timeFile, true).getChannel()) {
        timeWriter.writeToChannel(channel);
      }
      IndexIO.checkFileSize(timeFile);

      for (int i = 0; i < mergedDimensions.size(); ++i) {
        VSizeIndexedWriter writer = forwardDimWriters.get(i);
        writer.close();
        try (WritableByteChannel channel = Channels.newChannel(dimOuts.get(i).getOutput())) {
          writer.writeToChannel(channel);
        }
      }

      for (MetricColumnSerializer metWriter : metWriters) {
        metWriter.close();
      }

      ioPeon.cleanup();
      log.info(
          "outDir[%s] completed walk through of %,d rows in %,d millis.",
          v8OutDir,
          rowCount,
          System.currentTimeMillis() - startTime
      );

      /************ Create Inverted Indexes *************/
      startTime = System.currentTimeMillis();

      final File invertedFile = new File(v8OutDir, "inverted.drd");
      Files.touch(invertedFile);
      OutputSupplier<OutputStream> invertedOut = Files.asByteSink(invertedFile, FileWriteMode.APPEND);

      final File geoFile = new File(v8OutDir, "spatial.drd");
      Files.touch(geoFile);
      OutputSupplier<OutputStream> spatialOut = Files.asByteSink(geoFile, FileWriteMode.APPEND);

      for (int i = 0; i < mergedDimensions.size(); ++i) {
        long dimStartTime = System.currentTimeMillis();
        String dimension = mergedDimensions.get(i);

        File dimOutFile = dimOuts.get(i).getFile();
        final MappedByteBuffer dimValsMapped = Files.map(dimOutFile);

        final String string = SerializerUtils.readString(dimValsMapped);
        if (!dimension.equals(string)) {
          throw new ISE("dimensions[%s] didn't match with [%s]", dimension, string);
        }
        Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, ObjectStrategy.STRING_STRATEGY);
        log.info("Starting dimension[%s] with cardinality[%,d]", dimension, dimVals.size());

        final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
        ColumnPartWriter<ImmutableBitmap> writer = new GenericIndexedWriter<>(
            ioPeon, dimension, bitmapSerdeFactory.getObjectStrategy()
        );
        writer.open();

        boolean isSpatialDim = columnCapabilities.get(dimension).hasSpatialIndexes();
        ByteBufferWriter<ImmutableRTree> spatialWriter = null;
        RTree tree = null;
        IOPeon spatialIoPeon = new TmpFileIOPeon();
        final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
        if (isSpatialDim) {
          spatialWriter = new ByteBufferWriter<ImmutableRTree>(
              spatialIoPeon, dimension, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory)
          );
          spatialWriter.open();
          tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
        }

        final IndexSeeker[] dictIdSeeker = toIndexSeekers(indexes, dimConversions, dimension);

        //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
        for (int dictId = 0; dictId < dimVals.size(); dictId++) {
          progress.progress();
          final List<IntIterator> convertedInverteds = Lists.newArrayListWithCapacity(indexes.size());
          for (int j = 0; j < indexes.size(); ++j) {
            final int seekedDictId = dictIdSeeker[j].seek(dictId);
            if (seekedDictId != IndexSeeker.NOT_EXIST) {
              ImmutableBitmap bitmap = indexes.get(j).getBitmap(dimension, seekedDictId);
              if (bitmap == null) {
                continue;
              }
              if (rowNumConversions != null) {
                convertedInverteds.add(new IntIterators.Mapped(bitmap.iterator(), rowNumConversions[j]));
              } else {
                convertedInverteds.add(bitmap.iterator());
              }
            }
          }

          final MutableBitmap bitset = toBitmap(convertedInverteds, bitmapFactory.makeEmptyMutableBitmap());

          if (dictId == 0 && (Iterables.getFirst(dimVals, "") == null)) {
            bitset.or(nullRowsList.get(i));
          }

          writer.add(bitmapFactory.makeImmutableBitmap(bitset));

          if (isSpatialDim) {
            String dimVal = dimVals.get(dictId);
            if (dimVal != null) {
              List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
              float[] coords = new float[stringCoords.size()];
              for (int j = 0; j < coords.length; j++) {
                coords[j] = Float.valueOf(stringCoords.get(j));
              }
              tree.insert(coords, bitset);
            }
          }
        }
        writer.close();

        try (WritableByteChannel channel = Channels.newChannel(invertedOut.getOutput())) {
          SerializerUtils.writeString(channel, dimension);
          writer.writeToChannel(channel);
        }
        ioPeon.cleanup();

        log.info("Completed dimension[%s] in %,d millis.", dimension, System.currentTimeMillis() - dimStartTime);

        if (isSpatialDim) {
          spatialWriter.add(ImmutableRTree.newImmutableFromMutable(tree));
          spatialWriter.close();

          try (WritableByteChannel channel = Channels.newChannel(spatialOut.getOutput())) {
            SerializerUtils.writeString(channel, dimension);
            spatialWriter.writeToChannel(channel);
          }
          spatialIoPeon.cleanup();
        }
        ByteBufferUtils.unmap(dimValsMapped);
      }

      log.info("outDir[%s] completed inverted.drd in %,d millis.", v8OutDir, System.currentTimeMillis() - startTime);

      final ArrayList<String> expectedFiles = Lists.newArrayList(
          Iterables.concat(
              Arrays.asList(
                  "index.drd", "inverted.drd", "spatial.drd", String.format("time_%s.drd", IndexIO.BYTE_ORDER)
              ),
              Iterables.transform(mergedDimensions, GuavaUtils.formatFunction("dim_%s.drd")),
              Iterables.transform(
                  mergedMetrics, GuavaUtils.formatFunction(String.format("met_%%s_%s.drd", IndexIO.BYTE_ORDER))
              )
          )
      );

      if (segmentMetadata != null) {
        writeMetadataToFile(new File(v8OutDir, "metadata.drd"), segmentMetadata);
        log.info("wrote metadata.drd in outDir[%s].", v8OutDir);

        expectedFiles.add("metadata.drd");
      }

      Map<String, File> files = Maps.newLinkedHashMap();
      for (String fileName : expectedFiles) {
        files.put(fileName, new File(v8OutDir, fileName));
      }

      File smooshDir = new File(v8OutDir, "smoosher");
      smooshDir.mkdir();

      for (Map.Entry<String, File> entry : Smoosh.smoosh(v8OutDir, smooshDir, files).entrySet()) {
        entry.getValue().delete();
      }

      for (File file : smooshDir.listFiles()) {
        Files.move(file, new File(v8OutDir, file.getName()));
      }

      if (!smooshDir.delete()) {
        log.info("Unable to delete temporary dir[%s], contains[%s]", smooshDir, Arrays.asList(smooshDir.listFiles()));
        throw new IOException(String.format("Unable to delete temporary dir[%s]", smooshDir));
      }

      createIndexDrdFile(
          IndexIO.V8_VERSION,
          v8OutDir,
          GenericIndexed.fromIterable(mergedDimensions, ObjectStrategy.STRING_STRATEGY),
          GenericIndexed.fromIterable(mergedMetrics, ObjectStrategy.STRING_STRATEGY),
          dataInterval,
          indexSpec.getBitmapSerdeFactory()
      );

      indexIO.getDefaultIndexIOHandler().convertV8toV9(v8OutDir, outDir, indexSpec);
      return outDir;
    }
    finally {
      closer.close();
    }
  }

  protected Iterator<Rowboat> makeRowIterable(
      final List<IndexableAdapter> indexes,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final ArrayList<Map<String, IntBuffer>> dimConversions,
      final boolean[] convertMissingDimsFlags,
      final Function<ArrayList<Iterator<Rowboat>>, Iterator<Rowboat>> rowMergerFn
  )
  {
    final ArrayList<Iterator<Rowboat>> boats = Lists.newArrayListWithCapacity(indexes.size());

    for (int i = 0; i < indexes.size(); ++i) {
      final Map<String, IntBuffer> conversionMap = dimConversions.get(i);
      final IntBuffer[] conversions = FluentIterable
          .from(mergedDimensions)
          .transform(
              new Function<String, IntBuffer>()
              {
                @Override
                public IntBuffer apply(String input)
                {
                  return conversionMap.get(input);
                }
              }
          ).toArray(IntBuffer.class);

      Iterator<Rowboat> target = indexes.get(i).getRows(mergedDimensions, mergedMetrics).iterator();
      if (!Arrays.equals(convertMissingDimsFlags, new boolean[mergedDimensions.size()]) ||
          !Arrays.equals(conversions, new IntBuffer[mergedDimensions.size()])) {
        target = new DimConversionIterator(target, conversions, convertMissingDimsFlags);
      }
      boats.add(target);
    }

    return rowMergerFn.apply(boats);
  }

  // `values` is super set of `indexed`
  public static int[] toLookupMap(Indexed<String> indexed, List<String> values)
  {
    final int[] dimLookup = new int[values.size()];
    for (int i = 0; i < indexed.size(); i++) {
      dimLookup[i] = values.indexOf(indexed.get(i));
    }
    return dimLookup;
  }

  public static <T extends Comparable> ArrayList<T> mergeIndexed(final List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = Sets.newTreeSet(Ordering.<T>natural().nullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  public void createIndexDrdFile(
      byte versionId,
      File inDir,
      GenericIndexed<String> availableDimensions,
      GenericIndexed<String> availableMetrics,
      Interval dataInterval,
      BitmapSerdeFactory bitmapSerdeFactory
  ) throws IOException
  {
    File indexFile = new File(inDir, "index.drd");

    try (FileChannel channel = new FileOutputStream(indexFile).getChannel()) {
      channel.write(ByteBuffer.wrap(new byte[]{versionId}));

      availableDimensions.writeToChannel(channel);
      availableMetrics.writeToChannel(channel);
      SerializerUtils.writeString(
          channel, String.format("%s/%s", dataInterval.getStart(), dataInterval.getEnd())
      );
      SerializerUtils.writeString(
          channel, mapper.writeValueAsString(bitmapSerdeFactory)
      );
    }
    IndexIO.checkFileSize(indexFile);
  }

  protected IndexSeeker[] toIndexSeekers(
      List<IndexableAdapter> adapters,
      ArrayList<Map<String, IntBuffer>> dimConversions,
      String dimension
  )
  {
    IndexSeeker[] seekers = new IndexSeeker[adapters.size()];
    for (int i = 0; i < adapters.size(); i++) {
      IntBuffer dimConversion = dimConversions.get(i).get(dimension);
      if (dimConversion != null) {
        seekers[i] = new IndexSeekerWithConversion((IntBuffer) dimConversion.asReadOnlyBuffer().rewind());
      } else {
        Indexed<String> dimValueLookup = adapters.get(i).getDimValueLookup(dimension);
        seekers[i] = new IndexSeekerWithoutConversion(dimValueLookup == null ? 0 : dimValueLookup.size());
      }
    }
    return seekers;
  }

  static interface IndexSeeker
  {
    int NOT_EXIST = -1;
    int NOT_INIT = -1;

    int seek(int dictId);
  }

  static class IndexSeekerWithoutConversion implements IndexSeeker
  {
    private final int limit;

    public IndexSeekerWithoutConversion(int limit)
    {
      this.limit = limit;
    }

    @Override
    public int seek(int dictId)
    {
      return dictId < limit ? dictId : NOT_EXIST;
    }
  }

  /**
   * Get old dictId from new dictId, and only support access in order
   */
  static class IndexSeekerWithConversion implements IndexSeeker
  {
    private final IntBuffer dimConversions;
    private int currIndex;
    private int currVal;
    private int lastVal;

    IndexSeekerWithConversion(IntBuffer dimConversions)
    {
      this.dimConversions = dimConversions;
      this.currIndex = 0;
      this.currVal = NOT_INIT;
      this.lastVal = NOT_INIT;
    }

    @Override
    public int seek(int dictId)
    {
      if (dimConversions == null) {
        return NOT_EXIST;
      }
      if (lastVal != NOT_INIT) {
        if (dictId <= lastVal) {
          throw new ISE(
              "Value dictId[%d] is less than the last value dictId[%d] I have, cannot be.",
              dictId, lastVal
          );
        }
        return NOT_EXIST;
      }
      if (currVal == NOT_INIT) {
        currVal = dimConversions.get();
      }
      if (currVal == dictId) {
        int ret = currIndex;
        ++currIndex;
        if (dimConversions.hasRemaining()) {
          currVal = dimConversions.get();
        } else {
          lastVal = dictId;
        }
        return ret;
      } else if (currVal < dictId) {
        throw new ISE(
            "Skipped currValue dictId[%d], currIndex[%d]; incoming value dictId[%d]",
            currVal, currIndex, dictId
        );
      } else {
        return NOT_EXIST;
      }
    }
  }

  static class ConvertingIndexedInts implements Iterable<Integer>
  {
    private final ImmutableBitmap baseIndex;
    private final int[] conversion;

    public ConvertingIndexedInts(ImmutableBitmap baseIndex, int[] conversion)
    {
      this.baseIndex = baseIndex;
      this.conversion = conversion;
    }

    @Override
    public Iterator<Integer> iterator()
    {
      if (baseIndex == null) {
        return Iterators.emptyIterator();
      }
      return new Iterator<Integer>()
      {
        private final IntIterator iterator = baseIndex.iterator();

        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public Integer next()
        {
          return conversion == null ? iterator.next() : conversion[iterator.next()];
        }
      };
    }
  }

  public static class DimConversionIterator implements Iterator<Rowboat>
  {
    private static final int[] EMPTY_STR_DIM = new int[]{0};
    private final Iterator<Rowboat> iterator;

    DimConversionIterator(Iterator<Rowboat> rows, IntBuffer[] conversions, boolean[] convertMissingDimsFlags)
    {
      iterator = Iterators.transform(
          rows,
          new Function<Rowboat, Rowboat>()
          {
            @Override
            public Rowboat apply(Rowboat input)
            {
              final int[][] dims = input.getDims();
              for (int i = 0; i < conversions.length; ++i) {
                if (dims[i] == null && convertMissingDimsFlags[i]) {
                  dims[i] = EMPTY_STR_DIM;
                  continue;
                }
                final IntBuffer converter = conversions[i];
                if (converter == null) {
                  continue;     // no need conversion
                }
                for (int j = 0; j < dims[i].length; ++j) {
                  dims[i][j] = converter.get(dims[i][j]);
                }
              }

              return input;
            }
          }
      );
    }

    @Override
    public boolean hasNext()
    {
      return iterator.hasNext();
    }

    @Override
    public Rowboat next()
    {
      return iterator.next();
    }
  }

  public static class AggFactoryStringIndexed implements Indexed<String>
  {
    private final AggregatorFactory[] metricAggs;

    public AggFactoryStringIndexed(AggregatorFactory[] metricAggs) {this.metricAggs = metricAggs;}

    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public int size()
    {
      return metricAggs.length;
    }

    @Override
    public String get(int index)
    {
      return metricAggs[index].getName();
    }

    @Override
    public int indexOf(String value)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<String> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
  }

  public static class RowboatMergeFunction implements BinaryFn.Identical<Rowboat>
  {
    private final AggregatorFactory.Combiner[] metricAggs;

    public RowboatMergeFunction(AggregatorFactory.Combiner[] metricAggs)
    {
      this.metricAggs = metricAggs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Rowboat apply(Rowboat lhs, Rowboat rhs)
    {
      if (lhs == null) {
        return rhs;
      }
      if (rhs == null) {
        return lhs;
      }

      Object[] lhsMetrics = lhs.getMetrics();
      Object[] rhsMetrics = rhs.getMetrics();

      for (int i = 0; i < metricAggs.length; ++i) {
        Object lhsMetric = lhsMetrics[i];
        Object rhsMetric = rhsMetrics[i];
        if (lhsMetric == null) {
          lhsMetrics[i] = rhsMetric;
        } else if (rhsMetric != null) {
          lhsMetrics[i] = metricAggs[i].combine(lhsMetric, rhsMetric);
        }
      }
      return lhs;
    }
  }

  public static boolean isNullColumn(Iterable<String> dimValues)
  {
    if (dimValues == null) {
      return true;
    }
    for (String val : dimValues) {
      if (val != null) {
        return false;
      }
    }
    return true;
  }

  private void writeMetadataToFile(File metadataFile, Metadata metadata) throws IOException
  {
    try (FileOutputStream metadataFileOutputStream = new FileOutputStream(metadataFile);
         FileChannel metadataFilechannel = metadataFileOutputStream.getChannel()
    ) {
      byte[] metadataBytes = mapper.writeValueAsBytes(metadata);
      if (metadataBytes.length != metadataFilechannel.write(ByteBuffer.wrap(metadataBytes))) {
        throw new IOException("Failed to write metadata for file");
      }
    }
    IndexIO.checkFileSize(metadataFile);
  }

  static class DictionaryMergeIterator implements Iterator<String>
  {
    protected final IntBuffer[] conversions;
    protected final PriorityQueue<Pair<Integer, PeekingIterator<String>>> pQueue;

    protected int counter;

    DictionaryMergeIterator(Indexed<String>[] dimValueLookups, boolean useDirect)
    {
      pQueue = new ObjectHeapPriorityQueue<>(
          dimValueLookups.length,
          new Comparator<Pair<Integer, PeekingIterator<String>>>()
          {
            @Override
            public int compare(Pair<Integer, PeekingIterator<String>> lhs, Pair<Integer, PeekingIterator<String>> rhs)
            {
              return lhs.rhs.peek().compareTo(rhs.rhs.peek());
            }
          }
      );
      conversions = new IntBuffer[dimValueLookups.length];
      for (int i = 0; i < conversions.length; i++) {
        if (dimValueLookups[i] == null) {
          continue;
        }
        Indexed<String> indexed = dimValueLookups[i];
        if (useDirect) {
          conversions[i] = ByteBuffer.allocateDirect(indexed.size() * Ints.BYTES).asIntBuffer();
        } else {
          conversions[i] = IntBuffer.allocate(indexed.size());
        }

        final PeekingIterator<String> iter = Iterators.peekingIterator(
            Iterators.transform(
                indexed.iterator(),
                new Function<String, String>()
                {
                  @Override
                  public String apply(@Nullable String input)
                  {
                    return Strings.nullToEmpty(input);
                  }
                }
            )
        );
        if (iter.hasNext()) {
          pQueue.enqueue(Pair.of(i, iter));
        }
      }
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public String next()
    {
      final String value = writeTranslate(pQueue.first(), counter);
      while (!pQueue.isEmpty() && value.equals(pQueue.first().rhs.peek())) {
        writeTranslate(pQueue.first(), counter);
      }
      counter++;

      return value;
    }

    boolean needConversion(int index)
    {
      IntBuffer readOnly = conversions[index].asReadOnlyBuffer();
      readOnly.rewind();
      for (int i = 0; readOnly.hasRemaining(); i++) {
        if (i != readOnly.get()) {
          return true;
        }
      }
      return false;
    }

    private String writeTranslate(Pair<Integer, PeekingIterator<String>> smallest, int counter)
    {
      final int index = smallest.lhs;
      final String value = smallest.rhs.next();

      conversions[index].put(counter);
      if (smallest.rhs.hasNext()) {
        pQueue.changed();
      } else {
        pQueue.dequeue();
      }
      return value;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove");
    }
  }

  static class RowNumIterator implements Iterator<Rowboat>
  {
    private final PriorityQueue<Pair<Integer, PeekingIterator<Rowboat>>> pQueue;

    private final int[][] conversions;
    private final BinaryFn.Identical<Rowboat> merger;
    private final Ordering<Rowboat> ordering;

    private int rowNum;

    RowNumIterator(
        List<Iterator<Rowboat>> rowboats,
        int[][] conversions,
        BinaryFn.Identical<Rowboat> merger,
        Ordering<Rowboat> ordering
    )
    {
      Preconditions.checkArgument(rowboats.size() == conversions.length);
      pQueue = new ObjectHeapPriorityQueue<>(
          rowboats.size(),
          new Comparator<Pair<Integer, PeekingIterator<Rowboat>>>()
          {
            @Override
            public int compare(Pair<Integer, PeekingIterator<Rowboat>> lhs, Pair<Integer, PeekingIterator<Rowboat>> rhs)
            {
              return ordering.compare(lhs.rhs.peek(), rhs.rhs.peek());
            }
          }
      );
      this.conversions = conversions;
      for (int i = 0; i < conversions.length; i++) {
        final PeekingIterator<Rowboat> iter = Iterators.peekingIterator(rowboats.get(i));
        if (iter.hasNext()) {
          pQueue.enqueue(Pair.of(i, iter));
        }
      }
      this.merger = merger;
      this.ordering = ordering;
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public Rowboat next()
    {
      Rowboat value = writeTranslate(pQueue.first(), rowNum);
      if (merger != null) {
        while (!pQueue.isEmpty() && ordering.compare(value, pQueue.first().rhs.peek()) == 0) {
          value = merger.apply(value, writeTranslate(pQueue.first(), rowNum));
        }
      }
      rowNum++;

      return value;
    }

    private Rowboat writeTranslate(Pair<Integer, PeekingIterator<Rowboat>> smallest, int rowNum)
    {
      final int index = smallest.lhs;
      final Rowboat value = smallest.rhs.next();

      conversions[index][value.getRowNum()] = rowNum;
      if (smallest.rhs.hasNext()) {
        pQueue.changed();
      } else {
        pQueue.dequeue();
      }
      return value;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove");
    }
  }

  MutableBitmap toBitmap(final List<IntIterator> intIterators, final MutableBitmap bitmap)
  {
    if (intIterators.isEmpty()) {
      return bitmap;
    }
    if (intIterators.size() == 1) {
      final IntIterator iterator = intIterators.get(0);
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      return bitmap;
    }
    final IntIterator sorted = new IntIterators.Sorted(intIterators);
    int prev = -1;
    while (sorted.hasNext()) {
      final int next = sorted.next();
      if (next != prev) {
        bitmap.add(prev = next);
      }
    }
    return bitmap;
  }
}
