/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.io.smoosh;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.ISE;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Class that works in conjunction with FileSmoosher.  This class knows how to map in a set of files smooshed
 * by the FileSmoosher.
 */
public class SmooshedFileMapper implements Closeable
{
  public static SmooshedFileMapper load(File baseDir) throws IOException
  {
    return load(baseDir, false);
  }

  // Read the smoosh chunk files fully into HEAP ByteBuffers instead of memory-mapping them. Lets a segment be
  // queried purely from heap (no file/mmap backing) — the column readers are ByteBuffer-agnostic. For small or
  // transient segments only (heap/GC pressure); large segments should stay mmap'd.
  public static SmooshedFileMapper loadHeap(File baseDir) throws IOException
  {
    return load(baseDir, true);
  }

  private static SmooshedFileMapper load(File baseDir, boolean heap) throws IOException
  {
    File metaFile = FileSmoosher.metaFile(baseDir);

    BufferedReader in = null;
    try {
      in = Files.newReader(metaFile, Charsets.UTF_8);

      String line = in.readLine();
      if (line == null) {
        throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
      }

      String[] splits = line.split(",");
      if (!"v1".equals(splits[0])) {
        throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
      }
      if (splits.length != 3) {
        throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
      }
      int numFiles = Integer.valueOf(splits[2]);
      List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

      for (int i = 0; i < numFiles; ++i) {
        outFiles.add(FileSmoosher.chunkFile(baseDir, i));
      }

      Map<String, Metadata> internalFiles = Maps.newTreeMap();
      while ((line = in.readLine()) != null) {
        splits = line.split(",");

        if (splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
        }
        internalFiles.put(
            splits[0],
            new Metadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]))
        );
      }

      final SmooshedFileMapper mapper = new SmooshedFileMapper(baseDir, outFiles, internalFiles, heap);
      if (heap) {
        mapper.eagerLoadAll();   // pull every chunk into heap now, so baseDir may be deleted afterwards
      }
      return mapper;
    }
    finally {
      Closeables.close(in, false);
    }
  }

  // heap mode only: read all chunk files into heap ByteBuffers up front so the source files are no longer needed
  private void eagerLoadAll() throws IOException
  {
    for (int i = 0; i < outFiles.size(); i++) {
      while (buffersList.size() <= i) {
        buffersList.add(null);
      }
      buffersList.set(i, ByteBuffer.wrap(Files.toByteArray(outFiles.get(i))));
    }
  }

  /**
   * Build a purely in-memory (heap) mapper straight from the bytes of a segment's {@code index.zip} — no temp
   * files, no disk. Unzips in memory, parses meta.smoosh, and holds each chunk as a heap ByteBuffer. Pair with
   * {@code IndexIO.loadIndex(null, false, mapper)} (the mapper carries version.bin so no file read is needed).
   */
  public static SmooshedFileMapper loadHeapFromZip(byte[] indexZip) throws IOException
  {
    final Map<String, byte[]> files = Maps.newHashMap();
    try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(indexZip))) {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        files.put(entry.getName(), ByteStreams.toByteArray(zin));
      }
    }
    final byte[] metaBytes = files.get(FileSmoosher.metaFile(new File("")).getName());
    if (metaBytes == null) {
      throw new ISE("index.zip has no meta.smoosh");
    }
    final ParsedMeta meta = parseMeta(metaBytes);
    final List<ByteBuffer> chunks = Lists.newArrayListWithCapacity(meta.numChunks);
    for (int i = 0; i < meta.numChunks; i++) {
      final byte[] chunk = files.get(FileSmoosher.chunkFile(new File(""), i).getName());
      chunks.add(ByteBuffer.wrap(Preconditions.checkNotNull(chunk, "missing chunk %s", i)));
    }
    return fromMemory(meta.internalFiles, chunks, files.get("version.bin"));
  }

  /**
   * Build a mapper that serves each entry by RANGE-fetching its bytes on demand (header-first, no whole download).
   * Pass the small {@code meta.smoosh} bytes + {@code version.bin} bytes (both cheap to GET) and a
   * {@link RangeFetcher} over the chunk objects. Pair with {@code IndexIO.loadIndex(null, false, mapper)}: at load
   * only index.drd/metadata.drd ranges are fetched; each column's range is fetched on first getColumn().
   */
  public static SmooshedFileMapper fromRange(byte[] metaSmooshBytes, byte[] version, RangeFetcher fetcher) throws IOException
  {
    final ParsedMeta meta = parseMeta(metaSmooshBytes);
    final SmooshedFileMapper mapper = new SmooshedFileMapper(null, Arrays.asList(new File[meta.numChunks]), meta.internalFiles, true, version);
    mapper.rangeFetcher = fetcher;
    return mapper;
  }

  private static SmooshedFileMapper fromMemory(Map<String, Metadata> internalFiles, List<ByteBuffer> chunks, byte[] version)
  {
    final SmooshedFileMapper mapper = new SmooshedFileMapper(null, Arrays.asList(new File[chunks.size()]), internalFiles, true, version);
    mapper.buffersList.addAll(chunks);   // pre-populated -> mapFile never touches a file
    return mapper;
  }

  // ---- header bundle: version.bin + meta.smoosh + index.drd + metadata.drd combined into ONE object, so a
  //      segment's whole front block is read in a single GET at boot; only columns are range-fetched later ----

  /** Bundle a v9 segment dir's front block (version.bin + meta.smoosh + index.drd + metadata.drd) into one blob. */
  public static byte[] writeHeader(File segmentDir) throws IOException
  {
    final LinkedHashMap<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("version.bin", Files.toByteArray(FileSmoosher.versionFile(segmentDir)));
    final byte[] meta = Files.toByteArray(FileSmoosher.metaFile(segmentDir));
    entries.put("meta.smoosh", meta);
    final ParsedMeta pm = parseMeta(meta);
    for (String name : new String[]{"index.drd", "metadata.drd"}) {
      final Metadata md = pm.internalFiles.get(name);
      if (md != null) {
        entries.put(name, readRange(FileSmoosher.chunkFile(segmentDir, md.getFileNum()), md.getStartOffset(), md.getLength()));
      }
    }
    return pack(entries);
  }

  /**
   * Build a mapper from a {@link #writeHeader} bundle: version/meta/index.drd/metadata.drd are served from the
   * header (zero fetch); columns are range-fetched from the chunk object(s) via {@code columnFetcher} on first
   * access. Pair with {@code IndexIO.loadIndex(null, false, mapper)} for a single-GET boot.
   */
  public static SmooshedFileMapper fromHeader(byte[] header, RangeFetcher columnFetcher) throws IOException
  {
    final Map<String, ByteBuffer> bundle = unpack(header);
    final byte[] metaBytes = toBytes(bundle.get("meta.smoosh"));
    final byte[] version = toBytes(bundle.get("version.bin"));
    final ParsedMeta pm = parseMeta(metaBytes);
    final SmooshedFileMapper mapper = new SmooshedFileMapper(null, Arrays.asList(new File[pm.numChunks]), pm.internalFiles, true, version);
    mapper.rangeFetcher = columnFetcher;
    final Map<String, ByteBuffer> pre = new LinkedHashMap<>();
    for (String name : new String[]{"index.drd", "metadata.drd"}) {
      if (bundle.containsKey(name)) {
        pre.put(name, bundle.get(name));
      }
    }
    mapper.preloaded = pre;
    return mapper;
  }

  // Container header format version — versions the CONTAINER (packaging), independent of version.bin (the v9
  // columnar format, unchanged). Bump if the header layout changes.
  public static final int HEADER_FORMAT = 1;

  // A READABLE text directory (like meta.smoosh) followed by a blank line and the concatenated binary payloads:
  //   smoosh-header,v1,<numEntries>
  //   <name>,<payloadOffset>,<payloadLen>          (offset relative to the byte after the blank line)
  //   ...
  //   <blank line>
  //   <version.bin><meta.smoosh><index.drd><metadata.drd>   (binary)
  private static byte[] pack(LinkedHashMap<String, byte[]> entries) throws IOException
  {
    final StringBuilder dir = new StringBuilder();
    dir.append("smoosh-header,v").append(HEADER_FORMAT).append(',').append(entries.size()).append('\n');
    int off = 0;
    for (Map.Entry<String, byte[]> e : entries.entrySet()) {
      dir.append(e.getKey()).append(',').append(off).append(',').append(e.getValue().length).append('\n');
      off += e.getValue().length;
    }
    dir.append('\n');   // blank line ends the readable directory; binary payloads follow
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    bout.write(dir.toString().getBytes(StandardCharsets.UTF_8));
    for (byte[] payload : entries.values()) {
      bout.write(payload);
    }
    return bout.toByteArray();
  }

  private static Map<String, ByteBuffer> unpack(byte[] header) throws IOException
  {
    final int[] pos = {0};
    final String[] head = readLine(header, pos).split(",");
    if (!"smoosh-header".equals(head[0]) || !("v" + HEADER_FORMAT).equals(head[1])) {
      throw new ISE("unsupported header[%s]", String.join(",", head));
    }
    final int n = Integer.parseInt(head[2]);
    final String[] names = new String[n];
    final int[] offs = new int[n];
    final int[] lens = new int[n];
    for (int i = 0; i < n; i++) {
      final String[] s = readLine(header, pos).split(",");
      names[i] = s[0];
      offs[i] = Integer.parseInt(s[1]);
      lens[i] = Integer.parseInt(s[2]);
    }
    readLine(header, pos);   // blank line -> payloads start here
    final int payloadStart = pos[0];
    final Map<String, ByteBuffer> entries = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      entries.put(names[i], ByteBuffer.wrap(header, payloadStart + offs[i], lens[i]).slice());
    }
    return entries;
  }

  private static String readLine(byte[] buf, int[] pos)
  {
    int i = pos[0];
    while (i < buf.length && buf[i] != '\n') {
      i++;
    }
    final String line = new String(buf, pos[0], i - pos[0], StandardCharsets.UTF_8);
    pos[0] = i + 1;   // skip '\n'
    return line;
  }

  /** Split a {@link #writeHeader} bundle back into name -> bytes (used by a puller to reconstruct a segment dir). */
  public static Map<String, byte[]> unpackHeader(byte[] header) throws IOException
  {
    final Map<String, byte[]> out = new LinkedHashMap<>();
    for (Map.Entry<String, ByteBuffer> e : unpack(header).entrySet()) {
      out.put(e.getKey(), toBytes(e.getValue()));
    }
    return out;
  }

  /** Number of chunk (NNNNN.smoosh) files a segment has, from its meta.smoosh bytes. */
  public static int chunkCount(byte[] metaSmooshBytes) throws IOException
  {
    return parseMeta(metaSmooshBytes).numChunks;
  }

  /** The chunk file name for index {@code i} (e.g. "00000.smoosh"). */
  public static String chunkName(int i)
  {
    return FileSmoosher.chunkFile(new File(""), i).getName();
  }

  private static byte[] toBytes(ByteBuffer buffer)
  {
    final ByteBuffer dup = buffer.duplicate();
    final byte[] b = new byte[dup.remaining()];
    dup.get(b);
    return b;
  }

  private static byte[] readRange(File file, long offset, int length) throws IOException
  {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(offset);
      final byte[] b = new byte[length];
      raf.readFully(b);
      return b;
    }
  }

  private static ParsedMeta parseMeta(byte[] metaBytes) throws IOException
  {
    final Map<String, Metadata> internalFiles = Maps.newTreeMap();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metaBytes), Charsets.UTF_8))) {
      final String[] header = in.readLine().split(",");
      if (!"v1".equals(header[0]) || header.length != 3) {
        throw new ISE("bad meta.smoosh header[%s]", (Object) header);
      }
      final int numChunks = Integer.parseInt(header[2]);
      String line;
      while ((line = in.readLine()) != null) {
        final String[] s = line.split(",");
        internalFiles.put(s[0], new Metadata(Integer.parseInt(s[1]), Integer.parseInt(s[2]), Integer.parseInt(s[3])));
      }
      return new ParsedMeta(numChunks, internalFiles);
    }
  }

  private static final class ParsedMeta
  {
    private final int numChunks;
    private final Map<String, Metadata> internalFiles;

    private ParsedMeta(int numChunks, Map<String, Metadata> internalFiles)
    {
      this.numChunks = numChunks;
      this.internalFiles = internalFiles;
    }
  }

  private final File baseDir;
  private final List<File> outFiles;
  private final Map<String, Metadata> internalFiles;
  private final boolean heap;
  private final byte[] versionBytes;   // non-null only for in-memory mappers (no version.bin file to read)
  private RangeFetcher rangeFetcher;   // non-null for range-backed mappers: mapFile fetches each entry's bytes
  private Map<String, ByteBuffer> preloaded;   // entries served from the bundled header (index.drd/metadata.drd)
  // MappedByteBuffer when mmap'd, plain heap ByteBuffer when heap-loaded (both are ByteBuffer)
  private final List<ByteBuffer> buffersList = Lists.newArrayList();

  /**
   * Fetches an exact byte range of a smoosh chunk on demand (e.g. an S3 GET with a Range header). Lets a segment
   * be served header-first: only the small index.drd/metadata.drd ranges at load, then a column's range on first
   * access — the whole segment is never downloaded.
   */
  public interface RangeFetcher
  {
    ByteBuffer fetch(int fileNum, long offset, int length) throws IOException;
  }

  SmooshedFileMapper(File baseDir, List<File> outFiles, Map<String, Metadata> internalFiles)
  {
    this(baseDir, outFiles, internalFiles, false);
  }

  SmooshedFileMapper(File baseDir, List<File> outFiles, Map<String, Metadata> internalFiles, boolean heap)
  {
    this(baseDir, outFiles, internalFiles, heap, null);
  }

  SmooshedFileMapper(File baseDir, List<File> outFiles, Map<String, Metadata> internalFiles, boolean heap, byte[] versionBytes)
  {
    this.baseDir = baseDir;
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
    this.heap = heap;
    this.versionBytes = versionBytes;
  }

  /** version.bin content for in-memory mappers (so a loader need not read it from disk); null for file mappers. */
  public byte[] getVersion()
  {
    return versionBytes;
  }

  public File getBaseDir()
  {
    return baseDir;
  }

  public int getNumFiles()
  {
    return outFiles.size();
  }

  public Map<String, Metadata> getInternalFiles()
  {
    return internalFiles;
  }

  public Set<String> getInternalFilenames()
  {
    return internalFiles.keySet();
  }

  public ByteBuffer mapFile(String name) throws IOException
  {
    if (preloaded != null) {
      final ByteBuffer bundled = preloaded.get(name);
      if (bundled != null) {
        return bundled.duplicate();   // served from the header bundle — no fetch
      }
    }
    final Metadata metadata = internalFiles.get(name);
    return metadata == null ? null : mapFile(metadata);
  }

  /** True for a range-backed (header-first) mapper: columns are range-fetched on demand, never fully downloaded. */
  public boolean isRange()
  {
    return rangeFetcher != null;
  }

  /**
   * Range-fetch only the FIRST {@code len} bytes of a column (clamped to the column length). Used to pull a lucene
   * column's descriptor JSON + file-offset table without downloading the whole ~tens-of-MB index — the index files
   * are then range-read on demand via {@link #fetchInColumn}. Returns null if the column is absent; only valid on a
   * range mapper.
   */
  public ByteBuffer mapFileHead(String name, int len) throws IOException
  {
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }
    final long _t0 = System.nanoTime();
    final ByteBuffer buf = rangeFetcher.fetch(metadata.getFileNum(), metadata.getStartOffset(), Math.min(len, metadata.getLength()));
    io.druid.java.util.common.RangeProf.fetchNanos.addAndGet(System.nanoTime() - _t0);
    io.druid.java.util.common.RangeProf.fetchBytes.addAndGet(buf == null ? 0 : buf.remaining());
    io.druid.java.util.common.RangeProf.fetchCount.incrementAndGet();
    return buf;
  }

  /**
   * Range-fetch a sub-range of a column, {@code offset} being relative to the column's first byte. Backs the
   * on-demand reads of a range-served lucene index (term dictionary block + a term's postings) instead of fetching
   * the whole index extent. Only valid on a range mapper.
   */
  public ByteBuffer fetchInColumn(String name, long offset, int length) throws IOException
  {
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }
    final long _t0 = System.nanoTime();
    final ByteBuffer buf = rangeFetcher.fetch(metadata.getFileNum(), metadata.getStartOffset() + offset, length);
    io.druid.java.util.common.RangeProf.fetchNanos.addAndGet(System.nanoTime() - _t0);
    io.druid.java.util.common.RangeProf.fetchBytes.addAndGet(buf == null ? 0 : buf.remaining());
    io.druid.java.util.common.RangeProf.fetchCount.incrementAndGet();
    return buf;
  }

  private ByteBuffer mapFile(Metadata metadata) throws IOException
  {
    if (rangeFetcher != null) {
      final long _t0 = System.nanoTime();
      final ByteBuffer buf = rangeFetcher.fetch(metadata.getFileNum(), metadata.getStartOffset(), metadata.getLength());
      io.druid.java.util.common.RangeProf.fetchNanos.addAndGet(System.nanoTime() - _t0);
      io.druid.java.util.common.RangeProf.fetchBytes.addAndGet(buf == null ? 0 : buf.remaining());
      io.druid.java.util.common.RangeProf.fetchCount.incrementAndGet();
      return buf;
    }
    final int fileNum = metadata.getFileNum();
    while (buffersList.size() <= fileNum) {
      buffersList.add(null);
    }
    ByteBuffer buffer = buffersList.get(fileNum);
    if (buffer == null) {
      buffer = heap ? ByteBuffer.wrap(Files.toByteArray(outFiles.get(fileNum))) : Files.map(outFiles.get(fileNum));
      buffersList.set(fileNum, buffer);
    }
    return metadata.slice(buffer);
  }

  private static final String TIME_COLUMN = "__time";
  private static final Set<String> META_COLUMNS = Sets.newHashSet("index.drd", "metadata.drd");

  // hack for lazy mapping of index file
  public ByteBuffer mapFile(String name, boolean readOnly) throws IOException
  {
    if (heap || !readOnly) {
      return mapFile(name);   // heap mode reads everything into heap ByteBuffers
    }
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }
    // it's header and footer of smoosh file.. possibly cut the size of memory mapping
    if (TIME_COLUMN.equals(name)) {
      try (FileChannel channel = new RandomAccessFile(outFiles.get(metadata.getFileNum()), "r").getChannel()) {
        return channel.map(FileChannel.MapMode.READ_ONLY, metadata.getStartOffset(), metadata.getLength());
      }
    } else if (META_COLUMNS.contains(name)) {
      try (RandomAccessFile file = new RandomAccessFile(outFiles.get(metadata.getFileNum()), "r")) {
        file.seek(metadata.getStartOffset());
        final byte[] array = new byte[metadata.getLength()];
        file.readFully(array);
        return ByteBuffer.wrap(array);
      }
    } else {
      return mapFile(name);
    }
  }

  public Metadata getMetadata(String name)
  {
    return internalFiles.get(name);
  }

  public long getSerializedSize()
  {
    // skipping META_COLUMNS just cause it's hard to predict the size of merged metadata in SegmentMetadataQueryTest
    long serializedSize = 0L;
    for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
      if (!META_COLUMNS.contains(entry.getKey())) {
        serializedSize += entry.getValue().getLength();
      }
    }
    return serializedSize;
  }

  @Override
  public void close()
  {
    Throwable thrown = null;
    for (ByteBuffer buffer : buffersList) {
      if (!(buffer instanceof MappedByteBuffer)) {
        continue;   // heap buffers need no unmap; nulls skipped
      }
      try {
        ByteBufferUtils.unmap((MappedByteBuffer) buffer);
      }
      catch (Throwable t) {
        if (thrown == null) {
          thrown = t;
        } else {
          thrown.addSuppressed(t);
        }
      }
    }
    buffersList.clear();
    Throwables.propagateIfPossible(thrown);
  }
}
