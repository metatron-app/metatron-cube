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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.appenderator.AppenderatorPlumber;

import java.util.List;

public class Plumbers
{
  private static final Logger log = new Logger(Plumbers.class);

  private Plumbers()
  {
    // No instantiation
  }

  public static void addNextRowV2(
      final Supplier<Committer> committerSupplier,
      final FirehoseV2 firehose,
      final Plumber plumber,
      final boolean reportParseExceptions,
      final FireDepartmentMetrics metrics
  )
  {
    try {
      InputRow inputRow = firehose.currRow();
      addRow(committerSupplier, inputRow, plumber, reportParseExceptions, metrics);
    } catch (ParseException e) {
      if (reportParseExceptions) {
        throw e;
      } else {
        log.debug(e, "Discarded row due to exception, considering unparseable.");
        metrics.incrementUnparseable();
      }
    }
  }

  public static void addNextRow(
      final Supplier<Committer> committerSupplier,
      final Firehose firehose,
      final Plumber plumber,
      final boolean reportParseExceptions,
      final FireDepartmentMetrics metrics
  )
  {
    try {
      InputRow inputRow = firehose.nextRow();
      addRow(committerSupplier, inputRow, plumber, reportParseExceptions, metrics);
    } catch (ParseException e) {
      if (reportParseExceptions) {
        throw e;
      } else {
        log.debug(e, "Discarded row due to exception, considering unparseable.");
        metrics.incrementUnparseable();
      }
    }
  }

  private static void addRow(
      final Supplier<Committer> committerSupplier,
      final InputRow inputRow,
      final Plumber plumber,
      final boolean reportParseExceptions,
      final FireDepartmentMetrics metrics
  )
  {
    if (inputRow == null) {
      if (reportParseExceptions) {
        throw new ParseException("null input row");
      } else {
        log.debug("Discarded null input row, considering unparseable.");
        metrics.incrementUnparseable();
        return;
      }
    }

    try {
      // Included in ParseException try/catch, as additional parsing can be done during indexing.
      int numRows = plumber.add(inputRow, committerSupplier);

      if (numRows == -1) {
        metrics.incrementThrownAway();
        log.debug("Discarded row[%s], considering thrownAway.", inputRow);
        return;
      }

      metrics.incrementProcessed();
    } catch (ParseException e) {
      if (reportParseExceptions) {
        throw e;
      } else {
        log.debug(e, "Discarded row due to exception, considering unparseable.");
        metrics.incrementUnparseable();
      }
    } catch (IndexSizeExceededException e) {
      // Shouldn't happen if this is only being called by a single thread.
      // plumber.add should be swapping out indexes before they fill up.
      throw new ISE(e, "Index size exceeded, this shouldn't happen. Bad Plumber!");
    }
  }

  public static long estimatedFinishTime(Plumber plumber)
  {
    if (plumber instanceof RealtimePlumber) {
      return estimatedFinishTime(ImmutableList.<Sink>copyOf(((RealtimePlumber) plumber).getSinks().values()));
    }
    if (plumber instanceof AppenderatorPlumber) {
      return estimatedFinishTime(((AppenderatorPlumber) plumber).getSinks());
    }
    return -1;
  }

  private static long estimatedFinishTime(List<Sink> sinks)
  {
    if (GuavaUtils.isNullOrEmpty(sinks)) {
      return -1;
    }
    int totalRows = 0;
    int persistedRows = 0;
    long persistTime = 0;
    for (Sink sink : sinks) {
      for (FireHydrant hydrant : sink) {
        Segment segment = hydrant.getSegment();
        int numRows = segment.asStorageAdapter(false).getNumRows();
        if (segment instanceof QueryableIndexSegment) {
          if (hydrant.getPersistingTime() < 0) {
            continue;   // revived.. skip
          }
          persistTime += hydrant.getPersistingTime();
          persistedRows += numRows;
        }
        totalRows += numRows;
      }
    }
    if (totalRows > 0) {
      return (long) ((totalRows - persistedRows) / (double) totalRows * persistTime);
    }
    return -1;  // cannot know
  }
}
