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

package io.druid.segment.data;

import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 */
public interface IOPeon extends Closeable
{
  File getFile(String filename);

  OutputStream makeOutputStream(String filename) throws IOException;

  InputStream makeInputStream(String filename) throws IOException;

  default void copyTo(WritableByteChannel channel, String fileName) throws IOException
  {
    try (ReadableByteChannel input = Channels.newChannel(makeInputStream(fileName))) {
      FileSmoosher.transfer(channel, input);
    }
  }
}
