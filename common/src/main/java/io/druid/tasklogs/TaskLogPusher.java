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

package io.druid.tasklogs;

import com.google.common.io.Files;
import io.druid.java.util.common.logger.Logger;

import java.io.File;

/**
 * Something that knows how to persist local task logs to some form of long-term storage.
 */
public interface TaskLogPusher
{
  void pushTaskLog(String taskid, File logFile);

  default void writeToTmp(Logger LOG, String taskid, File logFile, Object supposed)
  {
    LOG.warn("Unable to create task log dir[%s]", supposed);
    try {
      File tempFile = File.createTempFile(taskid, ".log");
      Files.copy(logFile, tempFile);
      LOG.info("Wrote task log to temporary file: %s", tempFile);
    }
    catch (Throwable t) {
      LOG.info("Unable to write task log to temporary file.. discarding");
    }
  }
}
