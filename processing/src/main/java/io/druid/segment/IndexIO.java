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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.SerializerUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ColumnPartProvider.DictionarySupport;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizedIntSupplier;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.VSizedIndexedInt;
import io.druid.segment.data.VSizedInt;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnSupplier;
import io.druid.segment.serde.DoubleGenericColumnPartSerde;
import io.druid.segment.serde.DoubleGenericColumnSupplier;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnSupplier;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnSupplier;
import io.druid.segment.serde.SpatialIndexColumnPartSupplier;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexIO
{
  public static final byte V8_VERSION = 0x8;
  public static final byte V9_VERSION = 0x9;
  public static final int CURRENT_VERSION_ID = V9_VERSION;

  public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();
  public static final BitmapFactory CONCISE_FACTORY = new ConciseBitmapFactory();
  public static final Supplier<ImmutableBitmap> NO_NULLS = Suppliers.<ImmutableBitmap>ofInstance(
      CONCISE_FACTORY.makeEmptyImmutableBitmap()
  );

  private final Map<Integer, IndexLoader> indexLoaders;

  private static final EmittingLogger log = new EmittingLogger(IndexIO.class);

  private final ObjectMapper mapper;
  private final DefaultIndexIOHandler defaultIndexIOHandler;

  @Inject
  public IndexIO(ObjectMapper mapper)
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    defaultIndexIOHandler = new DefaultIndexIOHandler(mapper);
    indexLoaders = ImmutableMap.<Integer, IndexLoader>builder()
                               .put(0, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(1, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(2, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(3, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(4, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(5, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(6, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(7, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(8, new LegacyIndexLoader(defaultIndexIOHandler))
                               .put(9, new V9IndexLoader())
                               .build();


  }

  public ObjectMapper getObjectMapper()
  {
    return mapper;
  }

  public void validateTwoSegments(File dir1, File dir2) throws IOException
  {
    try (QueryableIndex queryableIndex1 = loadIndex(dir1)) {
      try (QueryableIndex queryableIndex2 = loadIndex(dir2)) {
        validateTwoSegments(
            new QueryableIndexIndexableAdapter(queryableIndex1),
            new QueryableIndexIndexableAdapter(queryableIndex2)
        );
      }
    }
  }

  public void validateTwoSegments(final IndexableAdapter adapter1, final IndexableAdapter adapter2)
  {
    if (adapter1.getNumRows() != adapter2.getNumRows()) {
      throw new SegmentValidationException(
          "Row count mismatch. Expected [%d] found [%d]",
          adapter1.getNumRows(),
          adapter2.getNumRows()
      );
    }
    {
      final Set<String> dimNames1 = Sets.newHashSet(adapter1.getDimensionNames());
      final Set<String> dimNames2 = Sets.newHashSet(adapter2.getDimensionNames());
      if (!dimNames1.equals(dimNames2)) {
        throw new SegmentValidationException(
            "Dimension names differ. Expected [%s] found [%s]",
            dimNames1,
            dimNames2
        );
      }
      final Set<String> metNames1 = Sets.newHashSet(adapter1.getMetricNames());
      final Set<String> metNames2 = Sets.newHashSet(adapter2.getMetricNames());
      if (!metNames1.equals(metNames2)) {
        throw new SegmentValidationException("Metric names differ. Expected [%s] found [%s]", metNames1, metNames2);
      }
    }
    final Iterator<Rowboat> it1 = adapter1.getRows().iterator();
    final Iterator<Rowboat> it2 = adapter2.getRows().iterator();
    long row = 0L;
    while (it1.hasNext()) {
      if (!it2.hasNext()) {
        throw new SegmentValidationException("Unexpected end of second adapter");
      }
      final Rowboat rb1 = it1.next();
      final Rowboat rb2 = it2.next();
      ++row;
      if (rb1.getRowNum() != rb2.getRowNum()) {
        throw new SegmentValidationException("Row number mismatch: [%d] vs [%d]", rb1.getRowNum(), rb2.getRowNum());
      }
      if (rb1.compareTo(rb2) != 0) {
        try {
          validateRowValues(rb1, adapter1, rb2, adapter2);
        }
        catch (SegmentValidationException ex) {
          throw new SegmentValidationException(ex, "Validation failure on row %d: [%s] vs [%s]", row, rb1, rb2);
        }
      }
    }
    if (it2.hasNext()) {
      throw new SegmentValidationException("Unexpected end of first adapter");
    }
    if (row != adapter1.getNumRows()) {
      throw new SegmentValidationException(
          "Actual Row count mismatch. Expected [%d] found [%d]",
          row,
          adapter1.getNumRows()
      );
    }
  }

  public QueryableIndex loadIndex(File inDir) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(inDir);

    final IndexLoader loader = indexLoaders.get(version);

    if (loader != null) {
      return loader.load(inDir, mapper);
    } else {
      throw new ISE("Unknown index version[%s]", version);
    }
  }

  public DataSegment decorateMeta(DataSegment segment, File directory) throws IOException
  {
    try (QueryableIndex index = loadIndex(directory)) {
      return segment
          .withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
          .withMetrics(Lists.newArrayList(index.getAvailableMetrics()))
          .withNumRows(index.getNumRows());
    }
  }

  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    try (InputStream in = new FileInputStream(indexFile)) {
      version = in.read();
    }
    return version;
  }

  public static void checkFileSize(File indexFile) throws IOException
  {
    final long fileSize = indexFile.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException(String.format("File[%s] too large[%s]", indexFile, fileSize));
    }
  }

  public boolean convertSegment(File toConvert, File converted, IndexSpec indexSpec) throws IOException
  {
    return convertSegment(toConvert, converted, indexSpec, false, true);
  }

  public boolean convertSegment(
      File toConvert,
      File converted,
      IndexSpec indexSpec,
      boolean forceIfCurrent,
      boolean validate
  ) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(toConvert);
    switch (version) {
      case 1:
      case 2:
      case 3:
        log.makeAlert("Attempt to load segment of version <= 3.")
           .addData("version", version)
           .emit();
        return false;
      case 4:
      case 5:
      case 6:
      case 7:
        log.info("Old version, re-persisting.");
        QueryableIndex segmentToConvert = loadIndex(toConvert);
        new IndexMerger(mapper, this).append(
            Arrays.<IndexableAdapter>asList(new QueryableIndexIndexableAdapter(segmentToConvert)),
            null,
            converted,
            indexSpec
        );
        return true;
      case 8:
        defaultIndexIOHandler.convertV8toV9(toConvert, converted, indexSpec);
        return true;
      default:
        if (forceIfCurrent) {
          new IndexMerger(mapper, this).convert(toConvert, converted, indexSpec);
          if (validate) {
            validateTwoSegments(toConvert, converted);
          }
          return true;
        } else {
          log.info("Version[%s], skipping.", version);
          return false;
        }
    }
  }

  public DefaultIndexIOHandler getDefaultIndexIOHandler()
  {
    return defaultIndexIOHandler;
  }

  static interface IndexIOHandler
  {
    public MMappedIndex mapDir(File inDir) throws IOException;
  }

  public static void validateRowValues(
      Rowboat rb1,
      IndexableAdapter adapter1,
      Rowboat rb2,
      IndexableAdapter adapter2
  )
  {
    if (rb1.getTimestamp() != rb2.getTimestamp()) {
      throw new SegmentValidationException(
          "Timestamp mismatch. Expected %d found %d",
          rb1.getTimestamp(), rb2.getTimestamp()
      );
    }
    final int[][] dims1 = rb1.getDims();
    final int[][] dims2 = rb2.getDims();
    if (dims1.length != dims2.length) {
      throw new SegmentValidationException(
          "Dim lengths not equal %s vs %s",
          Arrays.deepToString(dims1),
          Arrays.deepToString(dims2)
      );
    }
    final Indexed<String> dim1Names = adapter1.getDimensionNames();
    final Indexed<String> dim2Names = adapter2.getDimensionNames();
    for (int i = 0; i < dims1.length; ++i) {
      final int[] dim1Vals = dims1[i];
      final int[] dim2Vals = dims2[i];
      final String dim1Name = dim1Names.get(i);
      final String dim2Name = dim2Names.get(i);
      final Indexed<String> dim1ValNames = adapter1.getDimValueLookup(dim1Name);
      final Indexed<String> dim2ValNames = adapter2.getDimValueLookup(dim2Name);

      if (dim1Vals == null || dim2Vals == null) {
        if (dim1Vals != dim2Vals) {
          throw new SegmentValidationException(
              "Expected nulls, found %s and %s",
              Arrays.toString(dim1Vals),
              Arrays.toString(dim2Vals)
          );
        } else {
          continue;
        }
      }
      if (dim1Vals.length != dim2Vals.length) {
        // Might be OK if one of them has null. This occurs in IndexMakerTest
        if (dim1Vals.length == 0 && dim2Vals.length == 1) {
          final String dimValName = dim2ValNames.get(dim2Vals[0]);
          if (dimValName == null) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value [%s] is not null",
                dim2Name,
                dimValName
            );
          }
        } else if (dim2Vals.length == 0 && dim1Vals.length == 1) {
          final String dimValName = dim1ValNames.get(dim1Vals[0]);
          if (dimValName == null) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value [%s] is not null",
                dim1Name,
                dimValName
            );
          }
        } else {
          throw new SegmentValidationException(
              "Dim [%s] value lengths not equal. Expected %d found %d",
              dim1Name,
              dims1.length,
              dims2.length
          );
        }
      }

      for (int j = 0; j < Math.max(dim1Vals.length, dim2Vals.length); ++j) {
        final int dIdex1 = dim1Vals.length <= j ? -1 : dim1Vals[j];
        final int dIdex2 = dim2Vals.length <= j ? -1 : dim2Vals[j];

        if (dIdex1 == dIdex2) {
          continue;
        }

        final String dim1ValName = dIdex1 < 0 ? null : dim1ValNames.get(dIdex1);
        final String dim2ValName = dIdex2 < 0 ? null : dim2ValNames.get(dIdex2);
        if ((dim1ValName == null) || (dim2ValName == null)) {
          if ((dim1ValName == null) && (dim2ValName == null)) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value not equal. Expected [%s] found [%s]",
                dim1Name,
                dim1ValName,
                dim2ValName
            );
          }
        }

        if (!dim1ValName.equals(dim2ValName)) {
          throw new SegmentValidationException(
              "Dim [%s] value not equal. Expected [%s] found [%s]",
              dim1Name,
              dim1ValName,
              dim2ValName
          );
        }
      }
    }
  }

  public static class DefaultIndexIOHandler implements IndexIOHandler
  {
    private static final Logger log = new Logger(DefaultIndexIOHandler.class);
    private final ObjectMapper mapper;

    public DefaultIndexIOHandler(ObjectMapper mapper)
    {
      this.mapper = mapper;
    }

    @Override
    public MMappedIndex mapDir(File inDir) throws IOException
    {
      log.debug("Mapping v8 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(inDir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IllegalArgumentException(String.format("Unknown version[%s]", theVersion));
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> availableDimensions = GenericIndexed.read(
          indexBuffer, ObjectStrategy.STRING_STRATEGY
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer, ObjectStrategy.STRING_STRATEGY
      );
      final Interval dataInterval = new Interval(SerializerUtils.readString(indexBuffer));
      final BitmapSerdeFactory bitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();

      CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
          smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()), BYTE_ORDER
      );

      Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
      for (String metric : availableMetrics) {
        final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
        final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename));

        if (!metric.equals(holder.getName())) {
          throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
        }
        metrics.put(metric, holder);
      }

      Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
      Map<String, VSizedIndexedInt> dimColumns = Maps.newHashMap();
      Map<String, GenericIndexed<ImmutableBitmap>> bitmaps = Maps.newHashMap();

      for (String dimension : IndexedIterable.create(availableDimensions)) {
        ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
        String fileDimensionName = SerializerUtils.readString(dimBuffer);
        Preconditions.checkState(
            dimension.equals(fileDimensionName),
            "Dimension file[%s] has dimension[%s] in it!?",
            makeDimFile(inDir, dimension),
            fileDimensionName
        );

        dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, ObjectStrategy.STRING_STRATEGY));
        dimColumns.put(dimension, VSizedIndexedInt.readFromByteBuffer(dimBuffer));
      }

      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
      for (int i = 0; i < availableDimensions.size(); ++i) {
        bitmaps.put(
            SerializerUtils.readString(invertedBuffer),
            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
        );
      }

      Map<String, ImmutableRTree> spatialIndexed = Maps.newHashMap();
      ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexed.put(
            SerializerUtils.readString(spatialBuffer),
            ByteBufferSerializer.read(
                spatialBuffer,
                new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
            )
        );
      }

      final MMappedIndex retVal = new MMappedIndex(
          availableDimensions,
          availableMetrics,
          dataInterval,
          timestamps,
          metrics,
          dimValueLookups,
          dimColumns,
          bitmaps,
          spatialIndexed,
          smooshedFiles
      );

      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return retVal;
    }

    public void convertV8toV9(File v8Dir, File v9Dir, IndexSpec indexSpec)
        throws IOException
    {
      log.info("Converting v8[%s] to v9[%s]", v8Dir, v9Dir);

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(v8Dir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IAE("Unknown version[%s]", theVersion);
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper v8SmooshedFiles = Smoosh.map(v8Dir);

      v9Dir.mkdirs();
      final FileSmoosher v9Smoosher = new FileSmoosher(v9Dir);

      Files.asByteSink(new File(v9Dir, "version.bin")).write(Ints.toByteArray(9));
      Map<String, GenericIndexed<ImmutableBitmap>> bitmapIndexes = Maps.newHashMap();
      final ByteBuffer invertedBuffer = v8SmooshedFiles.mapFile("inverted.drd");
      BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

      while (invertedBuffer.hasRemaining()) {
        final String dimName = SerializerUtils.readString(invertedBuffer);
        bitmapIndexes.put(
            dimName,
            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
        );
      }

      Map<String, ImmutableRTree> spatialIndexes = Maps.newHashMap();
      final ByteBuffer spatialBuffer = v8SmooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexes.put(
            SerializerUtils.readString(spatialBuffer),
            ByteBufferSerializer.read(
                spatialBuffer, new IndexedRTree.ImmutableRTreeObjectStrategy(
                    bitmapSerdeFactory.getBitmapFactory()
                )
            )
        );
      }

      final LinkedHashSet<String> skippedFiles = Sets.newLinkedHashSet();
      final Set<String> skippedDimensions = Sets.newLinkedHashSet();
      for (String filename : v8SmooshedFiles.getInternalFilenames()) {
        log.info("Processing file[%s]", filename);
        if (filename.startsWith("dim_")) {
          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
          builder.setValueType(ValueDesc.STRING);

          final List<ByteBuffer> outParts = Lists.newArrayList();

          ByteBuffer dimBuffer = v8SmooshedFiles.mapFile(filename);
          String dimension = SerializerUtils.readString(dimBuffer);
          if (!filename.equals(String.format("dim_%s.drd", dimension))) {
            throw new ISE("loaded dimension[%s] from file[%s]", dimension, filename);
          }

          ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
          SerializerUtils.writeString(nameBAOS, dimension);
          outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));

          GenericIndexed<String> dictionary = GenericIndexed.read(
              dimBuffer, ObjectStrategy.STRING_STRATEGY
          );

          if (dictionary.size() == 0) {
            log.info("Dimension[%s] had cardinality 0, equivalent to no column, so skipping.", dimension);
            skippedDimensions.add(dimension);
            continue;
          }

          int emptyStrIdx = dictionary.indexOf("");
          List<Integer> singleValCol = null;
          VSizedIndexedInt multiValCol = VSizedIndexedInt.readFromByteBuffer(dimBuffer.asReadOnlyBuffer());
          GenericIndexed<ImmutableBitmap> bitmaps = bitmapIndexes.get(dimension);
          ImmutableRTree spatialIndex = spatialIndexes.get(dimension);

          final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
          boolean onlyOneValue = true;
          MutableBitmap nullsSet = null;
          for (int i = 0; i < multiValCol.size(); ++i) {
            VSizedInt rowValue = multiValCol.get(i);
            if (!onlyOneValue) {
              break;
            }
            if (rowValue.size() > 1) {
              onlyOneValue = false;
            }
            if (rowValue.size() == 0 || rowValue.get(0) == emptyStrIdx) {
              if (nullsSet == null) {
                nullsSet = bitmapFactory.makeEmptyMutableBitmap();
              }
              nullsSet.add(i);
            }
          }

          if (onlyOneValue) {
            log.info("Dimension[%s] is single value, converting...", dimension);
            final boolean bumpedDictionary;
            if (nullsSet != null) {
              log.info("Dimension[%s] has null rows.", dimension);
              final ImmutableBitmap theNullSet = bitmapFactory.makeImmutableBitmap(nullsSet);

              if (dictionary.get(0) != null) {
                log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);
                bumpedDictionary = true;
                final List<String> nullList = Lists.newArrayList();
                nullList.add(null);

                dictionary = GenericIndexed.fromIterable(
                    Iterables.concat(nullList, dictionary),
                    ObjectStrategy.STRING_STRATEGY
                );

                bitmaps = GenericIndexed.fromIterable(
                    Iterables.concat(Arrays.asList(theNullSet), bitmaps),
                    bitmapSerdeFactory.getObjectStrategy()
                );
              } else {
                bumpedDictionary = false;
                bitmaps = GenericIndexed.fromIterable(
                    Iterables.concat(
                        Arrays.asList(
                            DimFilters.union(bitmapFactory, Arrays.asList(theNullSet, bitmaps.get(0)))
                        ),
                        Iterables.skip(bitmaps, 1)
                    ),
                    bitmapSerdeFactory.getObjectStrategy()
                );
              }
            } else {
              bumpedDictionary = false;
            }

            final VSizedIndexedInt finalMultiValCol = multiValCol;
            singleValCol = new AbstractList<Integer>()
            {
              @Override
              public Integer get(int index)
              {
                final VSizedInt ints = finalMultiValCol.get(index);
                return ints.size() == 0 ? 0 : ints.get(0) + (bumpedDictionary ? 1 : 0);
              }

              @Override
              public int size()
              {
                return finalMultiValCol.size();
              }
            };

            multiValCol = null;
          } else {
            builder.setHasMultipleValues(true);
          }

          final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();

          final DictionaryEncodedColumnPartSerde.LegacySerializerBuilder columnPartBuilder = DictionaryEncodedColumnPartSerde
              .legacySerializerBuilder()
              .withDictionary(dictionary)
              .withBitmapSerdeFactory(bitmapSerdeFactory)
              .withBitmaps(bitmaps)
              .withSpatialIndex(spatialIndex)
              .withByteOrder(BYTE_ORDER);

          if (singleValCol != null) {
            if (compressionStrategy != null) {
              columnPartBuilder.withSingleValuedColumn(
                  CompressedVSizedIntSupplier.fromList(
                      singleValCol,
                      dictionary.size(),
                      CompressedVSizedIntSupplier.maxIntsInBufferForValue(dictionary.size()),
                      BYTE_ORDER,
                      compressionStrategy
                  )
              );
            } else {
              columnPartBuilder.withSingleValuedColumn(VSizedInt.fromList(singleValCol, dictionary.size()));
            }
          } else if (compressionStrategy != null) {
            columnPartBuilder.withMultiValuedColumn(
                CompressedVSizedIndexedIntSupplier.fromIterable(
                    multiValCol,
                    dictionary.size(),
                    BYTE_ORDER,
                    compressionStrategy
                )
            );
          } else {
            columnPartBuilder.withMultiValuedColumn(multiValCol);
          }
          builder.addSerde(columnPartBuilder.build());

          makeColumn(v9Smoosher, dimension, builder.build());

        } else if (filename.startsWith("met_")) {
          if (!filename.endsWith(String.format("%s.drd", BYTE_ORDER))) {
            skippedFiles.add(filename);
            continue;
          }

          MetricHolder holder = MetricHolder.fromByteBuffer(v8SmooshedFiles.mapFile(filename));
          final String metric = holder.getName();

          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();

          switch (holder.getType()) {
            case LONG:
              builder.setValueType(ValueDesc.LONG);
              builder.addSerde(new LongGenericColumnPartSerde(BYTE_ORDER, holder.longType));
              break;
            case FLOAT:
              builder.setValueType(ValueDesc.FLOAT);
              builder.addSerde(new FloatGenericColumnPartSerde(BYTE_ORDER, holder.floatType));
              break;
            case DOUBLE:
              builder.setValueType(ValueDesc.DOUBLE);
              builder.addSerde(new DoubleGenericColumnPartSerde(BYTE_ORDER, holder.doubleType));
              break;
            case COMPLEX:
              if (!(holder.complexType instanceof GenericIndexed)) {
                throw new ISE("Serialized complex types must be GenericIndexed objects.");
              }
              final GenericIndexed column = (GenericIndexed) holder.complexType;
              final String complexType = holder.getTypeName();
              builder.setValueType(ValueDesc.of(complexType));
              builder.addSerde(new ComplexColumnPartSerde(complexType, column));
              break;
            default:
              throw new ISE("Unknown type[%s]", holder.getType());
          }

          makeColumn(v9Smoosher, metric, builder.build());

        } else if (String.format("time_%s.drd", BYTE_ORDER).equals(filename)) {
          CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
              v8SmooshedFiles.mapFile(filename), BYTE_ORDER
          );

          ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
          builder.setValueType(ValueDesc.LONG);
          builder.addSerde(new LongGenericColumnPartSerde(BYTE_ORDER, timestamps));

          makeColumn(v9Smoosher, Column.TIME_COLUMN_NAME, builder.build());

        } else {
          skippedFiles.add(filename);
        }
      }

      final ByteBuffer indexBuffer = v8SmooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> dims8 = GenericIndexed.read(
          indexBuffer, ObjectStrategy.STRING_STRATEGY
      );
      final GenericIndexed<String> dims9 = GenericIndexed.fromIterable(
          Iterables.filter(
              dims8, new Predicate<String>()
              {
                @Override
                public boolean apply(String s)
                {
                  return !skippedDimensions.contains(s);
                }
              }
          ),
          ObjectStrategy.STRING_STRATEGY
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer, ObjectStrategy.STRING_STRATEGY
      );
      final Interval dataInterval = new Interval(SerializerUtils.readString(indexBuffer));
      final BitmapSerdeFactory segmentBitmapSerdeFactory = mapper.readValue(
          SerializerUtils.readString(indexBuffer),
          BitmapSerdeFactory.class
      );

      Set<String> columns = Sets.newTreeSet();
      columns.addAll(Lists.newArrayList(dims9));
      columns.addAll(Lists.newArrayList(availableMetrics));
      GenericIndexed<String> cols = GenericIndexed.fromIterable(columns, ObjectStrategy.STRING_STRATEGY);

      final String segmentBitmapSerdeFactoryString = mapper.writeValueAsString(segmentBitmapSerdeFactory);

      final long numBytes = cols.getSerializedSize() + dims9.getSerializedSize() + 16
                            + SerializerUtils.getSerializedStringByteSize(segmentBitmapSerdeFactoryString);
      final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
      cols.writeToChannel(writer);
      dims9.writeToChannel(writer);
      SerializerUtils.writeLong(writer, dataInterval.getStartMillis());
      SerializerUtils.writeLong(writer, dataInterval.getEndMillis());
      SerializerUtils.writeString(writer, segmentBitmapSerdeFactoryString);
      writer.close();

      final ByteBuffer metadataBuffer = v8SmooshedFiles.mapFile("metadata.drd");
      if (metadataBuffer != null) {
        v9Smoosher.add("metadata.drd", metadataBuffer);
      }

      log.info("Skipped files[%s]", skippedFiles);

      v9Smoosher.close();
    }

    protected final void makeColumn(
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
  }

  static interface IndexLoader
  {
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException;
  }

  static class LegacyIndexLoader implements IndexLoader
  {
    private final IndexIOHandler legacyHandler;

    LegacyIndexLoader(IndexIOHandler legacyHandler)
    {
      this.legacyHandler = legacyHandler;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      MMappedIndex index = legacyHandler.mapDir(inDir);

      Map<String, Supplier<Column>> columns = Maps.newHashMap();

      for (String dimension : index.getAvailableDimensions()) {
        VSizedIndexedInt column = index.getDimColumn(dimension);
        ColumnPartProvider<Dictionary<String>> dictionary = index.getDimValueLookup(dimension).asColumnPartProvider();
        ColumnBuilder builder = new ColumnBuilder()
            .setType(ValueDesc.STRING)
            .setHasMultipleValues(true)
            .setDictionaryEncodedColumn(
                new DictionaryEncodedColumnSupplier(
                    dictionary,
                    null,
                    ColumnPartProviders.<IndexedMultivalue<IndexedInts>>with(
                        column, column.getSerializedSize(), column.size()
                    ),
                    null
                )
            )
            .setBitmapIndex(
                new BitmapIndexColumnPartSupplier(
                    CONCISE_FACTORY,
                    index.getBitmapIndexes().get(dimension),
                    dictionary
                )
            );
        if (index.getSpatialIndexes().get(dimension) != null) {
          builder.setSpatialIndex(
              new SpatialIndexColumnPartSupplier(
                  index.getSpatialIndexes().get(dimension)
              )
          );
        }
        columns.put(dimension, Suppliers.ofInstance(builder.build(dimension)));
      }

      for (String metric : index.getAvailableMetrics()) {
        final MetricHolder metricHolder = index.getMetricHolder(metric);
        if (metricHolder.getType() == ValueType.FLOAT) {
          columns.put(
              metric,
              Suppliers.ofInstance(new ColumnBuilder()
                  .setType(ValueDesc.FLOAT)
                  .setGenericColumn(new FloatGenericColumnSupplier(metricHolder.floatType, NO_NULLS))
                  .build(metric)
              )
          );
        } else if (metricHolder.getType() == ValueType.DOUBLE) {
          columns.put(
              metric,
              Suppliers.ofInstance(new ColumnBuilder()
                  .setType(ValueDesc.DOUBLE)
                  .setGenericColumn(new DoubleGenericColumnSupplier(metricHolder.doubleType, NO_NULLS))
                  .build(metric)
              )
          );
        } else if (metricHolder.getType() == ValueType.COMPLEX) {
          columns.put(
              metric,
              Suppliers.ofInstance(new ColumnBuilder()
                  .setType(ValueDesc.of(metricHolder.getTypeName()))
                  .setComplexColumn(
                      new ComplexColumnPartSupplier(
                          metricHolder.getTypeName(), (GenericIndexed) metricHolder.complexType
                      )
                  )
                  .build(metric)
              )
          );
        }
      }

      Set<String> colSet = Sets.newTreeSet();
      for (String dimension : index.getAvailableDimensions()) {
        colSet.add(dimension);
      }
      for (String metric : index.getAvailableMetrics()) {
        colSet.add(metric);
      }

      String[] cols = colSet.toArray(new String[0]);
      columns.put(
          Column.TIME_COLUMN_NAME, Suppliers.ofInstance(new ColumnBuilder()
              .setType(ValueDesc.LONG)
              .setGenericColumn(new LongGenericColumnSupplier(index.timestamps, NO_NULLS))
              .build(Column.TIME_COLUMN_NAME)
          )
      );
      return new SimpleQueryableIndex(
          index.getInterval(),
          new ArrayIndexed<>(cols, String.class),
          index.getAvailableDimensions(),
          CONCISE_FACTORY,
          columns,
          null,
          index.getFileMapper(),
          null
      );
    }
  }

  static class V9IndexLoader implements IndexLoader
  {
    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      log.debug("Mapping v9 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
      if (theVersion != V9_VERSION) {
        throw new IllegalArgumentException(String.format("Expected version[9], got[%s]", theVersion));
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      /**
       * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
       * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
       */
      final GenericIndexed<String> cols = GenericIndexed.read(indexBuffer, ObjectStrategy.STRING_STRATEGY);
      final GenericIndexed<String> dims = GenericIndexed.read(indexBuffer, ObjectStrategy.STRING_STRATEGY);
      final Interval dataInterval = new Interval(indexBuffer.getLong(), indexBuffer.getLong());
      final BitmapSerdeFactory serdeFactory;

      /**
       * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
       * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
       * this information is appended to the end of index.drd.
       */
      if (indexBuffer.hasRemaining()) {
        serdeFactory = mapper.readValue(SerializerUtils.readString(indexBuffer), BitmapSerdeFactory.class);
      } else {
        serdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
      }
      final BitmapFactory bitmapFactory = serdeFactory.getBitmapFactory();

      Metadata metadata = null;
      ByteBuffer metadataBB = smooshedFiles.mapFile("metadata.drd");
      if (metadataBB != null) {
        try {
          metadata = mapper.readValue(
              SerializerUtils.readBytes(metadataBB, metadataBB.remaining()),
              Metadata.class
          );
        }
        catch (JsonParseException | JsonMappingException ex) {
          // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
          // is no longer supported then it is OK to not use the metadata instead of failing segment loading
          log.warn(ex, "Failed to load metadata for segment [%s]", inDir);
        }
        catch (IOException ex) {
          throw new IOException("Failed to read metadata", ex);
        }
      }

      Map<String, Supplier<Column>> columns = Maps.newHashMap();

      for (String columnName : GuavaUtils.concat(cols, Column.TIME_COLUMN_NAME)) {
        ByteBuffer mapped = smooshedFiles.mapFile(columnName);
        ColumnDescriptor descriptor = mapper.readValue(SerializerUtils.readString(mapped), ColumnDescriptor.class);
        columns.put(columnName, Suppliers.memoize(() -> descriptor.read(columnName, mapped, serdeFactory)));
      }

      // load cuboids
      Iterable<String> remaining = Iterables.filter(smooshedFiles.getInternalFilenames(), new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return !columns.containsKey(input);
        }
      });

      final Map<BigInteger, Pair<CuboidSpec, QueryableIndex>> cuboids = Maps.newTreeMap();
      for (Map.Entry<BigInteger, CuboidSpec> cuboid : Cuboids.extractCuboids(remaining).entrySet()) {
        final Map<String, Supplier<Column>> cuboidColumns = Maps.newTreeMap();
        final BigInteger cubeId = cuboid.getKey();
        final CuboidSpec cuboidSpec = cuboid.getValue();
        log.debug("-------> [%d] %s", cubeId, cuboidSpec);
        for (String dimension : cuboidSpec.getDimensions()) {
          cuboidColumns.put(dimension, Suppliers.memoize(() -> {
            Column source = Preconditions.checkNotNull(columns.get(dimension), dimension).get();
            try {
              ByteBuffer mapped = smooshedFiles.mapFile(Cuboids.dimension(cubeId, dimension));
              ColumnDescriptor desc = mapper.readValue(SerializerUtils.readString(mapped), ColumnDescriptor.class);
              Preconditions.checkArgument(source.getCapabilities().isDictionaryEncoded(), dimension);
              desc = desc.withParts(GuavaUtils.concat(desc.getParts(), delegateDictionary(source)));
              return desc.read(dimension, mapped, serdeFactory);
            }
            catch (Exception e) {
              log.warn(e, "Failed to load cuboid [%d] %s", cubeId, dimension);
              return null;
            }
          }));
        }
        for (Map.Entry<String, Set<String>> entry : cuboidSpec.getMetrics().entrySet()) {
          String metric = entry.getKey();
          for (String aggregator : entry.getValue()) {
            String column = metric.equals(Cuboids.COUNT_ALL_METRIC) ? metric : Cuboids.metricColumn(metric, aggregator);
            cuboidColumns.put(column, Suppliers.memoize(() -> {
              try {
                ByteBuffer mapped = smooshedFiles.mapFile(Cuboids.metric(cubeId, metric, aggregator));
                ColumnDescriptor descriptor = mapper.readValue(SerializerUtils.readString(mapped), ColumnDescriptor.class);
                return descriptor.read(metric, mapped, serdeFactory);
              }
              catch (Exception e) {
                log.warn(e, "Failed to load cuboid [%d] %s", cubeId, metric);
                return null;
              }
            }));
          }
        }
        ByteBuffer mapped = smooshedFiles.mapFile(Cuboids.dimension(cubeId, Column.TIME_COLUMN_NAME));
        ColumnDescriptor desc = mapper.readValue(SerializerUtils.readString(mapped), ColumnDescriptor.class);
        cuboidColumns.put(
            Column.TIME_COLUMN_NAME, Suppliers.memoize(() -> desc.read(Column.TIME_COLUMN_NAME, mapped, serdeFactory))
        );

        Indexed<String> dimensionNames = ListIndexed.ofString(cuboidSpec.getDimensions());
        Indexed<String> columnNames = ListIndexed.ofString(
            GuavaUtils.exclude(cuboidColumns.keySet(), Column.TIME_COLUMN_NAME)
        );
        QueryableIndex index = new SimpleQueryableIndex(
            dataInterval, columnNames, dimensionNames, bitmapFactory, cuboidColumns, null, null, null
        );
        cuboids.put(cubeId, Pair.of(cuboidSpec, index));
      }

      final QueryableIndex index = new SimpleQueryableIndex(
          dataInterval, cols, dims, bitmapFactory, columns, cuboids, smooshedFiles, metadata
      );

      log.debug("Mapped v9 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return index;
    }
  }

  public static File makeDimFile(File dir, String dimension)
  {
    return new File(dir, String.format("dim_%s.drd", dimension));
  }

  public static File makeTimeFile(File dir, ByteOrder order)
  {
    return new File(dir, String.format("time_%s.drd", order));
  }

  public static File makeMetricFile(File dir, String metricName, ByteOrder order)
  {
    return new File(dir, String.format("met_%s_%s.drd", metricName, order));
  }

  private static ColumnPartSerde delegateDictionary(final Column source)
  {
    return new ColumnPartSerde.Abstract()
    {
      @Override
      public ColumnPartSerde.Deserializer getDeserializer()
      {
        return new ColumnPartSerde.Deserializer()
        {
          @Override
          public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory)
          {
            DictionarySupport provider = Preconditions.checkNotNull(builder.getDictionaryEncodedColumn());
            builder.setDictionaryEncodedColumn(new DictionarySupport.Delegated(provider)
            {
              @Override
              public Dictionary<String> getDictionary()
              {
                return source.getDictionary();
              }

              @Override
              public DictionaryEncodedColumn get()
              {
                return super.get().withDictionary(source.getDictionary());
              }
            });
          }
        };
      }
    };
  }
}
