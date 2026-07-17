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

package org.apache.lucene.store;

import io.druid.java.util.common.RangeProf;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link IndexInput} whose bytes are range-fetched on demand (e.g. an S3 GET with a Range header) instead of read
 * from a pre-materialized buffer. Lets a range-served segment search its lucene index by pulling only the term
 * dictionary block + a term's postings (tens of KB) rather than the whole ~tens-of-MB index extent.
 *
 * <p>Extends {@link BufferedIndexInput}, which does the read-ahead buffering: it calls {@link #readInternal} to
 * refill a buffer ({@code bufferSize} bytes) whenever a read misses, so a cluster of small sequential lucene reads
 * collapses into one coarse fetch. Following the FS-directory pattern, {@code readInternal} reads at the absolute
 * offset {@code base + getFilePointer()} and {@code seekInternal} is a no-op (the file pointer is authoritative).
 */
public class RangeFetchIndexInput extends BufferedIndexInput
{
  private static final Logger LOG = new Logger(RangeFetchIndexInput.class);

  // Measurement scaffold (see RangeProf buffer-fill probe). Counters are always on (cheap atomics); this gates only
  // the verbose per-event log that prints every GET/prefetch offset so the two streams can be correlated by hand.
  private static final boolean PROBE_LOG = Boolean.getBoolean("druid.lucene.rangeProbeLog");

  /** Fetches exactly {@code length} bytes at {@code offset} (relative to the enclosing column's first byte). */
  public interface RangeSource
  {
    ByteBuffer fetch(long offset, int length) throws IOException;
  }

  private final RangeSource source;
  private final long base;     // absolute (column-relative) offset of this input's byte 0
  private final long length;
  private final int bufferSize;

  // Probe state: absolute end of the previous GET on THIS clone. A refill starting exactly here is a contiguous
  // (sequential) read; anywhere else means a seek happened and the previous buffer was likely under-consumed. Copied
  // by super.clone() — harmless (a clone just carries a stale hint that self-corrects on its first GET).
  private long probePrevEnd = -1;

  public RangeFetchIndexInput(String resourceDesc, RangeSource source, long base, long length, int bufferSize)
  {
    super(resourceDesc, bufferSize);
    this.source = source;
    this.base = base;
    this.length = length;
    this.bufferSize = bufferSize;
  }

  @Override
  protected void readInternal(ByteBuffer b) throws IOException
  {
    final int len = b.remaining();
    if (len == 0) {
      return;
    }
    final long pos = base + getFilePointer();
    probe(pos, len);
    final ByteBuffer data = source.fetch(pos, len);
    if (data == null || data.remaining() != len) {
      throw new IOException(
          "short range read at " + pos + ": wanted " + len + " got " + (data == null ? -1 : data.remaining()));
    }
    b.put(data);
  }

  // Classify this GET for the buffer-fill probe: a bulk read past the buffer (DIRECT, the 64K knob is irrelevant) vs a
  // buffer REFILL, whose contiguity with the previous GET tells over- (seek) vs under-fetch (seq). See RangeProf.
  private void probe(long pos, int len)
  {
    if (len > bufferSize) {
      RangeProf.bufDirectCount.incrementAndGet();
      RangeProf.bufDirectBytes.addAndGet(len);
    } else {
      RangeProf.bufRefillCount.incrementAndGet();
      RangeProf.bufRefillBytes.addAndGet(len);
      if (pos == probePrevEnd) {
        RangeProf.bufSeqRefills.incrementAndGet();
      } else {
        RangeProf.bufSeekRefills.incrementAndGet();
      }
    }
    probePrevEnd = pos + len;
    if (PROBE_LOG) {
      LOG.info("[range-probe] read %s off=%d len=%d%s", this, pos, len, len > bufferSize ? " DIRECT" : "");
    }
  }

  // Measurement-only override: Lucene's postings/term-dict readers call prefetch(fp, 1) to hint an upcoming read. The
  // base impl is a no-op; we keep it a no-op (no behavior change) and only record the hint so we can see, against the
  // read stream above, how often a prefetch precedes its read and whether hints arrive in overlappable batches.
  @Override
  public void prefetch(long offset, long length) throws IOException
  {
    RangeProf.prefetchCount.incrementAndGet();
    if (PROBE_LOG) {
      LOG.info("[range-probe] prefetch %s off=%d len=%d", this, base + offset, length);
    }
  }

  @Override
  protected void seekInternal(long pos)
  {
    // no-op: readInternal derives its absolute offset from getFilePointer()
  }

  @Override
  public long length()
  {
    return length;
  }

  @Override
  public RangeFetchIndexInput clone()
  {
    // super.clone() copies the buffered-read state (buffer contents + file pointer); the fetch source and the
    // [base,length) window are immutable and shared.
    return (RangeFetchIndexInput) super.clone();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long sliceLength) throws IOException
  {
    if (offset < 0 || sliceLength < 0 || offset + sliceLength > length) {
      throw new IllegalArgumentException(
          "slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" + sliceLength
          + ",fileLength=" + length + ": " + this);
    }
    return new RangeFetchIndexInput(sliceDescription, source, base + offset, sliceLength, bufferSize);
  }

  @Override
  public void close()
  {
  }
}
