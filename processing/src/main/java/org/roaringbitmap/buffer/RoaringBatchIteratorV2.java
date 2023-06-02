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

package org.roaringbitmap.buffer;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.ContainerBatchIterator;

import java.util.Arrays;

/**
 * copied to fix bug in clone (shared reference to iterator, causing premature cleanup of it)
 */
public final class RoaringBatchIteratorV2 implements BatchIterator {

  private final int initKey;
  private final int offset;
  private MappeableContainerPointer containerPointer;
  private int key;
  private ContainerBatchIterator iterator;
  private ArrayBatchIterator arrayBatchIterator;
  private BitmapBatchIterator bitmapBatchIterator;
  private RunBatchIterator runBatchIterator;

  public RoaringBatchIteratorV2(MappeableContainerPointer containerPointer) {
    this(containerPointer, 0);
  }

  public RoaringBatchIteratorV2(MappeableContainerPointer containerPointer, int offset) {
    this.containerPointer = containerPointer;
    this.initKey = containerPointer.hasContainer() ? containerPointer.key() << 16 : -1;
    this.offset = offset;
    nextIterator();
  }

  @Override
  public int nextBatch(int[] buffer) {
    int consumed = 0;
    if (iterator.hasNext()) {
      consumed += iterator.next(key, buffer);
      while (key == initKey && offset > 0) {
        if (buffer[consumed - 1] < offset) {
          consumed = iterator.next(key, buffer);
          continue;
        }
        int ix = Arrays.binarySearch(buffer, 0, consumed, offset);
        int skip = ix < 0 ? -ix - 1 : ix;
        consumed -= skip;
        if (skip > 0) {
          System.arraycopy(buffer, skip, buffer, 0, consumed);
        }
        break;
      }
      if (consumed > 0) {
        return consumed;
      }
    }
    containerPointer.advance();
    nextIterator();
    if (null != iterator) {
      return nextBatch(buffer);
    }
    return consumed;
  }

  @Override
  public boolean hasNext() {
    return null != iterator;
  }

  @Override
  public BatchIterator clone() {
    try {
      RoaringBatchIteratorV2 it = (RoaringBatchIteratorV2)super.clone();
      if (null != iterator) {
        it.iterator = iterator.clone();
      }
      if (null != containerPointer) {
        it.containerPointer = containerPointer.clone();
      }
      // these are should be cleared
      it.arrayBatchIterator = null;
      it.bitmapBatchIterator = null;
      it.runBatchIterator = null;
      return it;
    } catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException();
    }
  }

  private void nextIterator() {
    if (null != iterator) {
      iterator.releaseContainer();
    }
    if (null != containerPointer && containerPointer.hasContainer()) {
      MappeableContainer container = containerPointer.getContainer();
      if (container instanceof MappeableArrayContainer) {
        nextIterator((MappeableArrayContainer)container);
      } else if (container instanceof MappeableBitmapContainer) {
        nextIterator((MappeableBitmapContainer)container);
      } else if (container instanceof MappeableRunContainer) {
        nextIterator((MappeableRunContainer)container);
      }
      key = containerPointer.key() << 16;
    } else {
      iterator = null;
    }
  }

  private void nextIterator(MappeableArrayContainer array) {
    if (null == arrayBatchIterator) {
      arrayBatchIterator = new ArrayBatchIterator(array);
    } else {
      arrayBatchIterator.wrap(array);
    }
    iterator = arrayBatchIterator;
  }

  private void nextIterator(MappeableBitmapContainer bitmap) {
    if (null == bitmapBatchIterator) {
      bitmapBatchIterator = new BitmapBatchIterator(bitmap);
    } else {
      bitmapBatchIterator.wrap(bitmap);
    }
    iterator = bitmapBatchIterator;
  }

  private void nextIterator(MappeableRunContainer run) {
    if (null == runBatchIterator) {
      runBatchIterator = new RunBatchIterator(run);
    } else {
      runBatchIterator.wrap(run);
    }
    iterator = runBatchIterator;
  }
}
