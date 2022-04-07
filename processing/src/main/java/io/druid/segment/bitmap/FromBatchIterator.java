/*
 * Copyright 2011 - 2015 SK Telecom Group Inc.
 *
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

package io.druid.segment.bitmap;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.util.Arrays;

public class FromBatchIterator implements IntIterator
{
  public static IntIterator of(ImmutableRoaringBitmap bitmap)
  {
    return bitmap.isEmpty() ? IntIterators.EMPTY : new FromBatchIterator(bitmap.getBatchIterator());
  }

  private final BatchIterator iterator;
  private final int[] batch;
  private int valid;
  private int index;

  private FromBatchIterator(BatchIterator iterator)
  {
    this(iterator, new int[256], 0, 0);
  }

  private FromBatchIterator(BatchIterator iterator, int[] batch, int valid, int index)
  {
    this.iterator = iterator;
    this.batch = batch;
    this.valid = valid;
    this.index = index;
  }

  @Override
  public IntIterator clone()
  {
    return new FromBatchIterator(iterator.clone(), Arrays.copyOf(batch, batch.length), valid, index);
  }

  @Override
  public boolean hasNext()
  {
    if (index < valid) {
      return true;
    }
    index = valid = 0;  // reset
    while (iterator.hasNext() && (valid = iterator.nextBatch(batch)) == 0) {
    }
    return valid > 0;
  }

  @Override
  public int next()
  {
    return batch[index++];
  }
}
