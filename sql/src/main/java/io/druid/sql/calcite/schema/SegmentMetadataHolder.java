/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package io.druid.sql.calcite.schema;

import io.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;

/**
 * Immutable representation of RowSignature and other segment attributes needed by {@link SystemSchema.SegmentsTable}
 */
public class SegmentMetadataHolder
{

  // Booleans represented as long type, where 1 = true and 0 = false
  // to make it easy to count number of segments which are
  // published, available or realtime etc.
  private final long isPublished;
  private final long isAvailable;
  private final long isRealtime;
  private final String segmentId;
  private final long numReplicas;
  private final long numRows;
  @Nullable
  private final RowSignature rowSignature;

  public SegmentMetadataHolder(
      String segmentId,
      RowSignature rowSignature,
      long numRows,
      long isPublished,
      long isAvailable,
      long isRealtime,
      long numReplicas
  )
  {
    this.segmentId = segmentId;
    this.rowSignature = rowSignature;
    this.isPublished = isPublished;
    this.isAvailable = isAvailable;
    this.isRealtime = isRealtime;
    this.numReplicas = numReplicas;
    this.numRows = numRows;
  }

  public long isPublished()
  {
    return isPublished;
  }

  public long isAvailable()
  {
    return isAvailable;
  }

  public long isRealtime()
  {
    return isRealtime;
  }

  public String getSegmentId()
  {
    return segmentId;
  }

  public long getNumReplicas()
  {
    return numReplicas;
  }

  public long getNumRows()
  {
    return numRows;
  }

  @Nullable
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  public SegmentMetadataHolder withRowSignature(RowSignature rowSignature)
  {
    return new SegmentMetadataHolder(
        segmentId,
        rowSignature,
        numRows,
        isPublished,
        isAvailable,
        isRealtime,
        numReplicas
    );
  }

  public SegmentMetadataHolder withNumRows(long numRows)
  {
    return new SegmentMetadataHolder(
        segmentId,
        rowSignature,
        numRows,
        isPublished,
        isAvailable,
        isRealtime,
        numReplicas
    );
  }

  public SegmentMetadataHolder withNumReplicas(long numReplicas)
  {
    return new SegmentMetadataHolder(
        segmentId,
        rowSignature,
        numRows,
        isPublished,
        isAvailable,
        isRealtime,
        numReplicas
    );
  }
}
