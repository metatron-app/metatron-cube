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

import io.druid.common.utils.JodaUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

public class MessageTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    return new RejectionPolicy()
    {
      private volatile long maxTimestamp = JodaUtils.MIN_INSTANT;

      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime(maxTimestamp);
      }

      @Override
      public boolean accept(long timestamp)
      {
        maxTimestamp = Math.max(maxTimestamp, timestamp);

        return timestamp >= (maxTimestamp - windowMillis);
      }

      @Override
      public String toString()
      {
        return String.format("messageTime-%s", windowPeriod);
      }
    };
  }
}

