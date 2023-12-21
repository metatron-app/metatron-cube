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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Predicate;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;

/**
 * Provides a view on a firehose that only returns rows that match a certain predicate.
 * Not thread-safe.
 */
public class PredicateFirehose extends Firehose.Delegated
{
  private static final Logger log = new Logger(PredicateFirehose.class);
  private static final int IGNORE_THRESHOLD = 5000;
  private long ignored = 0;

  private final Predicate<InputRow> predicate;

  private InputRow savedInputRow = null;

  public PredicateFirehose(Firehose firehose, Predicate<InputRow> predicate)
  {
    super(firehose);
    this.predicate = predicate;
  }

  @Override
  public boolean hasMore()
  {
    if (savedInputRow != null) {
      return true;
    }

    while (delegate.hasMore()) {
      final InputRow row = delegate.nextRow();
      if (predicate.apply(row)) {
        savedInputRow = row;
        return true;
      }
      // Do not silently discard the rows
      if (ignored % IGNORE_THRESHOLD == 0) {
        log.warn("[%,d] InputRow(s) ignored as they do not satisfy the predicate", ignored);
      }
      ignored++;
    }

    return false;
  }

  @Override
  public InputRow nextRow()
  {
    final InputRow row = savedInputRow;
    savedInputRow = null;
    return row;
  }
}
