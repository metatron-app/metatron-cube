/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;
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
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    log.info("Starting coordination (%s). Getting available segments.", params.isMajorTick() ? "major" : "minor");

    // Display info about all available segments
    Supplier<Set<DataSegment>> supplier = new Supplier<Set<DataSegment>>()
    {
      @Override
      public Set<DataSegment> get()
      {
        final Set<DataSegment> availableSegments = coordinator.getOrderedAvailableDataSegments();
        log.info("Found [%,d] available segments.", availableSegments.size());
        return Collections.unmodifiableSet(availableSegments);
      }
    };

    return params.buildFromExisting()
                 .withAvailableSegments(supplier)
                 .build();
  }
}
