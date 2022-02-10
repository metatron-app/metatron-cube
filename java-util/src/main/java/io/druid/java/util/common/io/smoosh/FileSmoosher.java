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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MappedByteBufferHandler;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A class that concatenates files together into configurable sized chunks, works in conjunction
 * with the SmooshedFileMapper to provide access to the individual files.
 * <p/>
 * It does not split input files among separate output files, instead the various "chunk" files will
 * be varying sizes and it is not possible to add a file of size greater than Integer.MAX_VALUE
 */
public class FileSmoosher implements Closeable
{
  private static final String FILE_EXTENSION = "smoosh";
  private static final Joiner joiner = Joiner.on(",");

  private final File baseDir;
  private final int maxChunkSize;

  private final List<File> outFiles = Lists.newArrayList();
  private final Map<String, Metadata> internalFiles = Maps.newTreeMap();

  private OffsetWriter currOut = null;

  public FileSmoosher(
      File baseDir
  )
  {
    this(baseDir, Integer.MAX_VALUE);
  }

  public FileSmoosher(
      File baseDir,
      int maxChunkSize
  )
  {
    this.baseDir = baseDir;
    this.maxChunkSize = maxChunkSize;

    Preconditions.checkArgument(maxChunkSize > 0, "maxChunkSize must be a positive value.");
  }

  public void add(File fileToAdd) throws IOException
  {
    add(fileToAdd.getName(), fileToAdd);
  }

  public void add(String name, File fileToAdd) throws IOException
  {
    try (MappedByteBufferHandler fileMappingHandler = FileUtils.map(fileToAdd)) {
      add(name, fileMappingHandler.get());
    }
  }

  public void add(String name, ByteBuffer bufferToAdd) throws IOException
  {
    add(name, Arrays.asList(bufferToAdd));
  }

  public void add(String name, List<ByteBuffer> bufferToAdd) throws IOException
  {
    if (name.contains(",")) {
      throw new IAE("Cannot have a comma in the name of a file, got[%s].", name);
    }

    if (internalFiles.get(name) != null) {
      throw new IAE("Cannot add files of the same name, already have [%s]", name);
    }

    long size = 0;
    for (ByteBuffer buffer : bufferToAdd) {
      size += buffer.remaining();
    }

    try (SmooshedWriter out = addWithSmooshedWriter(name, size)) {
      for (ByteBuffer buffer : bufferToAdd) {
        out.write(buffer);
      }
    }
  }

  public SmooshedWriter addWithSmooshedWriter(final String name, final long size) throws IOException
  {
    if (size > maxChunkSize) {
      throw new IAE("Asked to add buffers[%,d] larger than configured max[%,d]", size, maxChunkSize);
    }
    if (currOut == null) {
      currOut = getNewCurrOut();
    }
    if (currOut.bytesLeft() < size) {
      currOut.close();
      currOut = getNewCurrOut();
    }

    final int startOffset = currOut.getCurrOffset();

    // verifier
    return new SmooshedWriter()
    {
      private boolean open = true;
      private long bytesWritten = 0;

      @Override
      public int write(ByteBuffer in) throws IOException
      {
        return Ints.checkedCast(verifySize(currOut.write(in)));
      }

      @Override
      public long transferFrom(FileChannel src) throws IOException
      {
        return verifySize(currOut.transferFrom(src));
      }

      private long verifySize(long bytesWrittenInChunk) throws IOException
      {
        bytesWritten += bytesWrittenInChunk;

        if (bytesWritten != currOut.getCurrOffset() - startOffset) {
          throw new ISE("Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten > size) {
          throw new ISE("Wrote[%,d] bytes for something of size[%,d]", bytesWritten, size);
        }

        return bytesWrittenInChunk;
      }

      @Override
      public boolean isOpen()
      {
        return open;
      }

      @Override
      public void close() throws IOException
      {
        open = false;
        internalFiles.put(name, new Metadata(currOut.getFileNum(), startOffset, currOut.getCurrOffset()));

        if (bytesWritten != currOut.getCurrOffset() - startOffset) {
          throw new ISE("Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten != size) {
          throw new IOException(
              String.format("Expected [%,d] bytes, only saw [%,d], potential corruption?", size, bytesWritten)
          );
        }
      }
    };
  }

  @Override
  public void close() throws IOException
  {
    if (currOut != null) {
      currOut.close();
    }

    File metaFile = metaFile(baseDir);

    try (Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), Charsets.UTF_8))) {
      out.write(String.format("v1,%d,%d", maxChunkSize, outFiles.size()));
      out.write("\n");

      for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
        final Metadata metadata = entry.getValue();
        out.write(
            joiner.join(
                entry.getKey(),
                metadata.getFileNum(),
                metadata.getStartOffset(),
                metadata.getEndOffset()
            )
        );
        out.write("\n");
      }
    }
  }

  private OffsetWriter getNewCurrOut() throws FileNotFoundException
  {
    final int fileNum = outFiles.size();
    File outFile = makeChunkFile(baseDir, fileNum);
    outFiles.add(outFile);
    return new OffsetWriter(fileNum, new FileOutputStream(outFile), maxChunkSize);
  }

  public static File metaFile(File baseDir)
  {
    return new File(baseDir, String.format("meta.%s", FILE_EXTENSION));
  }

  public static File makeChunkFile(File baseDir, int i)
  {
    return new File(baseDir, String.format("%05d.%s", i, FILE_EXTENSION));
  }

  public static class OffsetWriter implements SmooshedWriter
  {
    private final int fileNum;
    private final int maxLength;
    private final FileChannel channel;

    private int currOffset = 0;

    OffsetWriter(int fileNum, FileOutputStream output, int maxLength)
    {
      this.fileNum = fileNum;
      this.channel = output.getChannel();
      this.maxLength = maxLength;
    }

    public int getFileNum()
    {
      return fileNum;
    }

    public int getCurrOffset()
    {
      return currOffset;
    }

    public int bytesLeft()
    {
      return maxLength - currOffset;
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException
    {
      return Ints.checkedCast(addToOffset(channel.write(buffer)));
    }

    @Override
    public long transferFrom(FileChannel src) throws IOException
    {
      channel.position(currOffset + src.size());   // needed to grow file.. don't know why
      return addToOffset(channel.transferFrom(src, currOffset, src.size()));
    }

    private long addToOffset(long numBytesWritten) throws IOException
    {
      if (numBytesWritten > bytesLeft()) {
        throw new ISE("Wrote more bytes[%,d] than available[%,d]. Don't do that.", numBytesWritten, bytesLeft());
      }
      currOffset += numBytesWritten;
      return numBytesWritten;
    }

    @Override
    public boolean isOpen()
    {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
      channel.close();
    }

    public void sync() throws IOException
    {
      channel.force(true);
    }
  }

  public SmooshedFileMapper asMapped(File baseDir) throws IOException
  {
    if (currOut != null) {
      currOut.sync();
    }
    return new SmooshedFileMapper(baseDir, ImmutableList.copyOf(outFiles), ImmutableMap.copyOf(internalFiles));
  }
}
