/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
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

package io.druid.indexing.overlord.helpers;

import com.google.inject.Inject;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.logger.Logger;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.tasklogs.TaskLogKiller;
import org.joda.time.Duration;

import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class TaskLogAutoCleaner implements OverlordHelper
{
  private static final Logger log = new Logger(TaskLogAutoCleaner.class);

  private final TaskLogKiller taskLogKiller;
  private final TaskLogAutoCleanerConfig config;
  private final TaskStorage taskStorage;

  @Inject
  public TaskLogAutoCleaner(
      TaskLogKiller taskLogKiller,
      TaskLogAutoCleanerConfig config,
      TaskStorage taskStorage
  )
  {
    this.taskLogKiller = taskLogKiller;
    this.config = config;
    this.taskStorage = taskStorage;
  }

  @Override
  public boolean isEnabled()
  {
    return config.isEnabled();
  }

  @Override
  public void schedule(ScheduledExecutorService exec)
  {
    log.info("Scheduling TaskLogAutoCleaner with config [%s].", config.toString());

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        Duration.millis(config.getInitialDelay()),
        Duration.millis(config.getDelay()),
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              long timestamp = System.currentTimeMillis() - config.getDurationToRetain();
              taskLogKiller.killOlderThan(timestamp);
              taskStorage.removeTasksOlderThan(timestamp);
            }
            catch (Exception ex) {
              log.error(ex, "Failed to clean-up the task logs");
            }
          }
        }
    );
  }
}
