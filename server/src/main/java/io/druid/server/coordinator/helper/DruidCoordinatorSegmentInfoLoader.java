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

package io.druid.server.coordinator.helper;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Set;

public class DruidCoordinatorSegmentInfoLoader implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  private static final Logger log = new Logger(DruidCoordinatorSegmentInfoLoader.class);

  public DruidCoordinatorSegmentInfoLoader(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(final DruidCoordinatorRuntimeParams params)
  {
    return params.buildFromExisting()
                 .withAvailableSegments(coordinator.getAvailableDataSegments())
                 .build();
  }
}
