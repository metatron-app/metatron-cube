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

import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.Direction;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

public class SingleScanTimeDimSelector implements DimensionSelector
{
  private final ExtractionFn extractionFn;
  private final LongColumnSelector selector;
  private final Direction direction;

  private final Map<Integer, String> timeValues = Maps.newHashMap();
  private String currentValue = null;
  private long currentTimestamp = Long.MIN_VALUE;
  private int index = -1;


  // Use a special DimSelector for projected time columns
  // - it assumes time values are scanned once and values are grouped together
  //   (i.e. we never revisit a timestamp we have seen before, unless it is the same as the last accessed one)
  // - it also applies and caches extraction function values at the DimSelector level to speed things up
  public SingleScanTimeDimSelector(LongColumnSelector selector, ExtractionFn extractionFn, Direction direction)
  {
    if (extractionFn == null) {
      throw new UnsupportedOperationException("time dimension must provide an extraction function");
    }

    this.extractionFn = extractionFn;
    this.selector = selector;
    this.direction = direction;
  }

  @Override
  public IndexedInts getRow()
  {
    // if this the first timestamp, apply and cache extraction function result
    final long timestamp = selector.get();
    if (index < 0) {
      currentTimestamp = timestamp;
      currentValue = extractionFn.apply(timestamp);
      ++index;
      timeValues.put(index, currentValue);
    }
    // if this is a new timestamp, apply and cache extraction function result
    // since timestamps are assumed grouped and scanned once, we only need to
    // check if the current timestamp is different than the current timestamp.
    //
    // If this new timestamp is mapped to the same value by the extraction function,
    // we can also avoid creating a dimension value and corresponding index
    // and use the current one
    else if (timestamp != currentTimestamp) {
      if (direction == Direction.ASCENDING ? timestamp < currentTimestamp : timestamp > currentTimestamp) {
        // re-using this selector for multiple scans would cause the same rows to return different IDs
        // we might want to re-visit if we ever need to do multiple scans with this dimension selector
        throw new IllegalStateException("cannot re-use time dimension selector for multiple scans");
      }
      currentTimestamp = timestamp;
      final String value = extractionFn.apply(timestamp);
      if (!value.equals(currentValue)) {
        currentValue = value;
        ++index;
        timeValues.put(index, currentValue);
      }
      // Note: this could be further optimized by checking if the new value is one we have
      // previously seen, but would require keeping track of both the current and the maximum index
    }
    // otherwise, if the current timestamp is the same as the previous timestamp,
    // keep using the same dimension value index

    return IndexedInts.from(index);
  }

  @Override
  public int getValueCardinality()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  public Object lookupName(int id)
  {
    if (id == index) {
      return currentValue;
    } else {
      return timeValues.get(id);
    }
  }

  @Override
  public ValueDesc type()
  {
    return ValueDesc.STRING;
  }

  @Override
  public int lookupId(Object name)
  {
    throw new UnsupportedOperationException("time column does not support lookups");
  }
}
