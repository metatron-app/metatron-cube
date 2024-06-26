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

package io.druid.server.coordinator;

import io.druid.timeline.DataSegment;

/**
 */
@Deprecated
public class BalancerSegmentHolder
{
  private final ServerHolder fromServer;
  private final DataSegment segment;

  public BalancerSegmentHolder(ServerHolder fromServer, DataSegment segment)
  {
    this.fromServer = fromServer;
    this.segment = segment;
  }

  public ServerHolder getServerHolder()
  {
    return fromServer;
  }

  public DataSegment getSegment()
  {
    return segment;
  }
}
