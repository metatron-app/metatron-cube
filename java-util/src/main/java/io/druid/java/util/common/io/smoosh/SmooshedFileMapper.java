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
import io.druid.java.util.common.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.ISE;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

      return new SmooshedFileMapper(baseDir, outFiles, internalFiles, heap);
    }
    finally {
      Closeables.close(in, false);
    }
  }

  private final File baseDir;
  private final List<File> outFiles;
  private final Map<String, Metadata> internalFiles;
  private final boolean heap;
  // MappedByteBuffer when mmap'd, plain heap ByteBuffer when heap-loaded (both are ByteBuffer)
  private final List<ByteBuffer> buffersList = Lists.newArrayList();

  SmooshedFileMapper(File baseDir, List<File> outFiles, Map<String, Metadata> internalFiles)
  {
    this(baseDir, outFiles, internalFiles, false);
  }

  SmooshedFileMapper(File baseDir, List<File> outFiles, Map<String, Metadata> internalFiles, boolean heap)
  {
    this.baseDir = baseDir;
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
    this.heap = heap;
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
    final Metadata metadata = internalFiles.get(name);
    return metadata == null ? null : mapFile(metadata);
  }

  private ByteBuffer mapFile(Metadata metadata) throws IOException
  {
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
