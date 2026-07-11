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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.common.guava.DSuppliers;
import io.druid.common.utils.SerializerUtils;
import io.druid.data.ValueType;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.LazyCapabilitiesColumn;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The container's <b>readable</b> header (format v2): a fully text front-index for a v9 segment, so a range-served
 * historical can boot and answer schema/capability probes from ONE small object — no binary index.drd/metadata.drd
 * and no column payloads. Layout (all CSV/JSON, no binary blobs):
 * <pre>
 *   smoosh-header,v2
 *   version,9
 *   interval,&lt;startMillis&gt;,&lt;endMillis&gt;
 *   bitmap,{"type":"roaring"}
 *   metadata,{...Metadata JSON...}
 *   columns,&lt;index.drd column names, comma-separated&gt;
 *   dimensions,&lt;index.drd dimension names, comma-separated&gt;
 *   caps,&lt;K&gt;
 *   &lt;name,type,typeName,dict,runlen,bitmap,spatial,multival,metricbitmap,bitsliced,fst,external&gt;   x K
 *   meta
 *   &lt;verbatim meta.smoosh text to end&gt;
 * </pre>
 * The per-column capability rows are the front-index: {@link #load} serves getColumnCapabilities/asSignature from
 * them via {@link LazyCapabilitiesColumn}, so only columns a query actually reads get their payload range-fetched.
 * The v9 columnar format is unchanged — column payloads are still decoded by the normal {@link ColumnDescriptor}.
 */
public class ContainerHeader
{
  public static final String MAGIC = "smoosh-header,v2";

  // Head range-fetched for a range-served lucene index-only column: must cover the descriptor JSON + the lucene
  // file-offset table (a name + two ints per index file, ~tens of files). 256KB is generous; mapFileHead clamps to
  // the column length, so smaller columns fetch whole.
  private static final int LUCENE_HEAD_BYTES = 256 * 1024;

  /** Build the readable v2 header from a persisted v9 segment dir (called at push time; the dir is local). */
  public static byte[] write(File segmentDir, ObjectMapper mapper) throws IOException
  {
    try (SmooshedFileMapper smoosh = SmooshedFileMapper.load(segmentDir)) {
      final ByteBuffer index = smoosh.mapFile("index.drd");
      final GenericIndexed<String> cols = GenericIndexed.readString(index);
      final GenericIndexed<String> dims = GenericIndexed.readString(index);
      final long startMillis = index.getLong();
      final long endMillis = index.getLong();
      final String bitmapJson = index.hasRemaining() ? SerializerUtils.readString(index) : "";
      final BitmapSerdeFactory serdeFactory =
          Strings.isNullOrEmpty(bitmapJson) ? BitmapSerde.createLegacyFactory()
                                            : mapper.readValue(bitmapJson, BitmapSerdeFactory.class);

      final ByteBuffer metaBB = smoosh.mapFile("metadata.drd");
      final String metadataJson = metaBB == null ? "" : new String(
          SerializerUtils.readBytes(metaBB, metaBB.remaining()), StandardCharsets.UTF_8);

      final List<String> colNames = toList(cols);
      final List<String> dimNames = toList(dims);

      final List<String> allForCaps = Lists.newArrayList(Column.TIME_COLUMN_NAME);
      allForCaps.addAll(colNames);

      final StringBuilder sb = new StringBuilder();
      sb.append(MAGIC).append('\n');
      sb.append("version,").append(SegmentUtils.getVersionFromDir(segmentDir)).append('\n');
      sb.append("interval,").append(startMillis).append(',').append(endMillis).append('\n');
      sb.append("bitmap,").append(bitmapJson).append('\n');
      sb.append("metadata,").append(metadataJson).append('\n');
      sb.append("columns,").append(String.join(",", colNames)).append('\n');
      sb.append("dimensions,").append(String.join(",", dimNames)).append('\n');
      sb.append("caps,").append(allForCaps.size()).append('\n');
      // dict sub-ranges: for plain dictionary-encoded columns, the absolute (fileNum,offset,length) of the
      // dictionary within the chunk objects, so a range reader can fetch JUST the distinct values (e.g. to build
      // an in-memory per-segment value set / bloom for segment pruning) without pulling the encoded ints/bitmaps.
      final List<String> dictRows = Lists.newArrayList();
      for (String name : allForCaps) {
        final ByteBuffer buf = smoosh.mapFile(name);
        final int colStart = buf.position();                       // column blob start in the buffer's index space
        final ColumnDescriptor desc = mapper.readValue(SerializerUtils.readString(buf), ColumnDescriptor.class);
        final int partStart = buf.position();                      // first part-serde (after the descriptor JSON)
        final ColumnCapabilities caps = desc.read(name, buf, serdeFactory).getCapabilities();
        sb.append(capsRow(name, caps)).append('\n');
        if (caps.isDictionaryEncoded() && !hasExternalIndex(caps)) {
          final ByteBuffer dbuf = buf.duplicate();
          dbuf.position(partStart);
          final int[] r = DictionaryEncodedColumnPartSerde.dictionaryRange(dbuf);   // [offset-in-part, length]
          if (r != null) {
            final io.druid.java.util.common.io.smoosh.Metadata md = smoosh.getInternalFiles().get(name);
            final long absOffset = md.getStartOffset() + (partStart - colStart) + r[0];
            dictRows.add(name + ',' + md.getFileNum() + ',' + absOffset + ',' + r[1]);
          }
        }
      }
      sb.append("dicts,").append(dictRows.size()).append('\n');
      for (String row : dictRows) {
        sb.append(row).append('\n');
      }
      sb.append("meta\n");

      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
      // meta.smoosh is a top-level dir file (not an internal smoosh entry) — append it verbatim as the tail
      out.write(Files.toByteArray(FileSmoosher.metaFile(segmentDir)));
      return out.toByteArray();
    }
  }

  /** True if these header bytes are the readable v2 format. */
  public static boolean isV2(byte[] header)
  {
    return header.length >= MAGIC.length()
           && new String(header, 0, MAGIC.length(), StandardCharsets.UTF_8).equals(MAGIC);
  }

  /**
   * Build a range-served QueryableIndex from a v2 header + a column {@link SmooshedFileMapper.RangeFetcher}.
   * Nothing is fetched here: capabilities/schema come from the header; each column's payload is range-fetched
   * (and v9-decoded) only on first value access.
   */
  public static QueryableIndex load(byte[] header, SmooshedFileMapper.RangeFetcher fetcher, ObjectMapper mapper)
      throws IOException
  {
    final Parsed p = parse(header);
    final BitmapSerdeFactory serdeFactory =
        Strings.isNullOrEmpty(p.bitmapJson) ? BitmapSerde.createLegacyFactory()
                                            : mapper.readValue(p.bitmapJson, BitmapSerdeFactory.class);
    final Metadata metadata =
        Strings.isNullOrEmpty(p.metadataJson) ? null : mapper.readValue(p.metadataJson, Metadata.class);
    final SmooshedFileMapper colMapper = SmooshedFileMapper.fromRange(p.metaSmoosh, versionBytes(p.version), fetcher);

    final Map<String, Supplier<Column>> columns = new LinkedHashMap<>();
    for (Map.Entry<String, ColumnCapabilities> e : p.caps.entrySet()) {
      final String name = e.getKey();
      final ColumnCapabilities caps = e.getValue();
      final Supplier<Column> delegate = DSuppliers.memoize(() -> {
        // Range-serve a lucene index WITHOUT downloading it: fetch only the column head (descriptor + the index
        // file-offset table) and let the lucene reader range-read each index file on demand (term-dict block + a
        // term's postings, tens of KB) rather than the whole ~tens-of-MB index. Only safe when the lucene index IS
        // the entire payload — an index-only column, i.e. a single part; a base value column would need its own
        // bytes not present in the head, so those (and non-lucene external indexes) fall back to a whole fetch.
        if (caps.hasLuceneIndex()) {
          final ByteBuffer head = colMapper.mapFileHead(name, LUCENE_HEAD_BYTES);
          final ColumnDescriptor desc = mapper.readValue(SerializerUtils.readString(head), ColumnDescriptor.class);
          if (desc.getParts().size() == 1) {
            return desc.read(new ColumnBuilder(name).setRangeSource(colMapper, name), head, serdeFactory).build();
          }
        }
        final ByteBuffer buf = colMapper.mapFile(name);
        return mapper.readValue(SerializerUtils.readString(buf), ColumnDescriptor.class)
                     .read(name, buf, serdeFactory);
      });
      // A column carrying an external index (lucene / dictionary-FST) is exposed as a REAL column: the query-time
      // lucene `specialize` does an extension-classloader swap keyed on the real column's index-key class, which a
      // capability wrapper would defeat (the swap can't trigger, and the filter can't be rewritten). Plain columns
      // ARE wrapped so their capabilities/schema come from the header — their payload is fetched only if a query
      // reads them.
      if (hasExternalIndex(caps)) {
        columns.put(name, delegate);
      } else {
        columns.put(name, DSuppliers.memoize(() -> new LazyCapabilitiesColumn(name, caps, delegate)));
      }
    }

    return new SimpleQueryableIndex(
        new Interval(p.startMillis, p.endMillis),
        ListIndexed.ofString(p.columns),
        ListIndexed.ofString(p.dimensions),
        serdeFactory.getBitmapFactory(),
        columns,
        null,
        colMapper,
        metadata
    );
  }

  /** For download-mode: the raw meta.smoosh bytes carried in a v2 header (to reconstruct the local dir). */
  public static byte[] metaSmoosh(byte[] header) throws IOException
  {
    return parse(header).metaSmoosh;
  }

  /** For download-mode: the 4-byte version.bin content carried in a v2 header. */
  public static byte[] versionBin(byte[] header) throws IOException
  {
    return versionBytes(parse(header).version);
  }

  // ---- helpers ----

  private static List<String> toList(GenericIndexed<String> indexed)
  {
    final List<String> list = Lists.newArrayListWithCapacity(indexed.size());
    for (int i = 0; i < indexed.size(); i++) {
      list.add(indexed.get(i));
    }
    return list;
  }

  private static String capsRow(String name, ColumnCapabilities c)
  {
    final List<String> ext = c.getExternalIndices();
    return String.join(",",
        name,
        c.getType() == null ? "" : c.getType().name(),
        Strings.nullToEmpty(c.getTypeName()),
        b(c.isDictionaryEncoded()),
        b(c.isRunLengthEncoded()),
        b(c.hasBitmapIndexes()),
        b(c.hasSpatialIndexes()),
        b(c.hasMultipleValues()),
        b(c.hasMetricBitmap()),
        b(c.hasBitSlicedBitmap()),
        b(c.hasDictionaryFST()),
        ext == null || ext.isEmpty() ? "" : String.join(";", ext)
    );
  }

  private static ColumnCapabilities parseCaps(String[] f)
  {
    return new ColumnCapabilities()
        .setType(Strings.isNullOrEmpty(f[1]) ? null : ValueType.valueOf(f[1]))
        .setTypeName(Strings.emptyToNull(f[2]))
        .setDictionaryEncoded(bool(f[3]))
        .setRunLengthEncoded(bool(f[4]))
        .setHasBitmapIndexes(bool(f[5]))
        .setHasSpatialIndexes(bool(f[6]))
        .setHasMultipleValues(bool(f[7]))
        .setHasMetricBitmap(bool(f[8]))
        .setHasBitSlicedBitmap(bool(f[9]))
        .setHasDictionaryFST(bool(f[10]))
        .setExternalIndices(f.length > 11 && !f[11].isEmpty() ? Arrays.asList(f[11].split(";")) : null);
  }

  private static boolean hasExternalIndex(ColumnCapabilities c)
  {
    return c.hasLuceneIndex()
           || c.hasDictionaryFST()
           || (c.getExternalIndices() != null && !c.getExternalIndices().isEmpty());
  }

  private static String b(boolean v) { return v ? "1" : "0"; }
  private static boolean bool(String s) { return "1".equals(s); }

  private static byte[] versionBytes(int version)
  {
    return new byte[]{
        (byte) (version >>> 24), (byte) (version >>> 16), (byte) (version >>> 8), (byte) version
    };
  }

  private static final class Parsed
  {
    int version;
    long startMillis;
    long endMillis;
    String bitmapJson = "";
    String metadataJson = "";
    List<String> columns = Collections.emptyList();
    List<String> dimensions = Collections.emptyList();
    final Map<String, ColumnCapabilities> caps = new LinkedHashMap<>();
    final Map<String, long[]> dictRanges = new LinkedHashMap<>();   // column -> {fileNum, offset, length}
    byte[] metaSmoosh;
  }

  /**
   * Per-column dictionary sub-ranges carried in the header: {@code column -> {fileNum, absoluteOffset, length}}
   * into the chunk objects. Empty for headers written before this section existed (backward compatible). A range
   * reader fetches {@code fetcher.fetch((int)r[0], r[1], (int)r[2])} then {@link DictionaryEncodedColumnPartSerde#readDictionary}.
   */
  public static Map<String, long[]> dictRanges(byte[] header) throws IOException
  {
    return parse(header).dictRanges;
  }

  private static Parsed parse(byte[] header) throws IOException
  {
    final Parsed p = new Parsed();
    final int[] pos = {0};
    if (!MAGIC.equals(line(header, pos))) {
      throw new IOException("not a v2 container header");
    }
    int capCount = -1;
    while (pos[0] < header.length) {
      final String raw = line(header, pos);
      final int comma = raw.indexOf(',');
      final String key = comma < 0 ? raw : raw.substring(0, comma);
      final String val = comma < 0 ? "" : raw.substring(comma + 1);
      if ("version".equals(key)) {
        p.version = Integer.parseInt(val.trim());
      } else if ("interval".equals(key)) {
        final String[] s = val.split(",");
        p.startMillis = Long.parseLong(s[0].trim());
        p.endMillis = Long.parseLong(s[1].trim());
      } else if ("bitmap".equals(key)) {
        p.bitmapJson = val;
      } else if ("metadata".equals(key)) {
        p.metadataJson = val;
      } else if ("columns".equals(key)) {
        p.columns = val.isEmpty() ? Collections.emptyList() : Arrays.asList(val.split(","));
      } else if ("dimensions".equals(key)) {
        p.dimensions = val.isEmpty() ? Collections.emptyList() : Arrays.asList(val.split(","));
      } else if ("caps".equals(key)) {
        capCount = Integer.parseInt(val.trim());
        for (int i = 0; i < capCount; i++) {
          final String[] f = line(header, pos).split(",", -1);
          p.caps.put(f[0], parseCaps(f));
        }
      } else if ("dicts".equals(key)) {
        final int dictCount = Integer.parseInt(val.trim());
        for (int i = 0; i < dictCount; i++) {
          final String[] f = line(header, pos).split(",");
          p.dictRanges.put(f[0], new long[]{Long.parseLong(f[1]), Long.parseLong(f[2]), Long.parseLong(f[3])});
        }
      } else if ("meta".equals(key)) {
        p.metaSmoosh = Arrays.copyOfRange(header, pos[0], header.length);
        break;
      }
    }
    return p;
  }

  private static String line(byte[] buf, int[] pos)
  {
    int i = pos[0];
    while (i < buf.length && buf[i] != '\n') {
      i++;
    }
    final String s = new String(buf, pos[0], i - pos[0], StandardCharsets.UTF_8);
    pos[0] = i + 1;
    return s;
  }
}
