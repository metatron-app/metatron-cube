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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.collections.IntList;
import org.joda.time.DateTime;

import java.util.Arrays;

public class Rowboat implements Comparable<Rowboat>
{
  private final long timestamp;
  private final int[][] dims;
  private final Object[] metrics;
  private final IntList comprisedRows;

  public Rowboat(
      long timestamp,
      int[][] dims,
      Object[] metrics,
      int indexNum,
      int rowNum
  )
  {
    this(timestamp, dims, metrics, new IntList(indexNum, rowNum));
  }

  private Rowboat(long timestamp, int[][] dims, Object[] metrics, IntList comprisedRows)
  {
    this.timestamp = timestamp;
    this.dims = dims;
    this.metrics = metrics;
    this.comprisedRows = comprisedRows;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public int[][] getDims()
  {
    return dims;
  }

  public Object[] getMetrics()
  {
    return metrics;
  }

  public void comprised(IntList comprising)
  {
    comprisedRows.addAll(comprising);
  }

  public IntList getComprisedRows()
  {
    return comprisedRows;
  }

  public void applyRowMapping(int[][] conversions, int rowNum)
  {
    for (int i = 0; i < comprisedRows.size(); i += 2) {
      conversions[comprisedRows.get(i)][comprisedRows.get(i + 1)] = rowNum;
    }
  }

  public int getIndexNum()
  {
    return comprisedRows.get(0);
  }

  public int getRowNum()
  {
    return comprisedRows.get(1);
  }

  @Override
  public int compareTo(Rowboat rhs)
  {
    int retVal = Longs.compare(timestamp, rhs.timestamp);

    if (retVal == 0) {
      retVal = Ints.compare(dims.length, rhs.dims.length);
    }

    int index = 0;
    while (retVal == 0 && index < dims.length) {
      int[] lhsVals = dims[index];
      int[] rhsVals = rhs.dims[index];

      if (lhsVals == null) {
        if (rhsVals == null) {
          index++;
          continue;
        }
        return -1;
      }

      if (rhsVals == null) {
        return 1;
      }

      retVal = Ints.compare(lhsVals.length, rhsVals.length);

      int valsIndex = 0;
      while (retVal == 0 && valsIndex < lhsVals.length) {
        retVal = Ints.compare(lhsVals[valsIndex], rhsVals[valsIndex]);
        ++valsIndex;
      }
      ++index;
    }

    return retVal;
  }

  public Rowboat withDims(int[][] newDims)
  {
    return new Rowboat(timestamp, newDims, metrics, comprisedRows);
  }

  public Rowboat withDimsAndMetrics(int[][] newDims, Object[] newMetrics)
  {
    return new Rowboat(timestamp, newDims, newMetrics, comprisedRows);
  }

  @Override
  public String toString()
  {
    return "Rowboat{" +
           "timestamp=" + new DateTime(timestamp).toString() +
           ", dims=" + Arrays.deepToString(dims) +
           ", metrics=" + Arrays.toString(metrics) +
           ", comprisedRows=" + comprisedRows +
           '}';
  }
}
