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

import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import org.apache.commons.io.FileUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 */
public class TmpFileIOPeon implements IOPeon
{
  private final File base;
  private final boolean allowOverwrite;

  private final Map<String, File> createdFiles = Maps.newHashMap();

  public TmpFileIOPeon(String prefix, boolean allowOverwrite) throws IOException
  {
    this.base = GuavaUtils.createTemporaryDirectory(prefix, "filePeon");
    this.allowOverwrite = allowOverwrite;
  }

  @Override
  public File getFile(String filename)
  {
    return createdFiles.get(filename);
  }

  @Override
  public File makeOutputFile(String filename)
  {
    File file = createdFiles.get(filename);
    if (file == null) {
      createdFiles.put(filename, file = new File(base, filename));
      file.deleteOnExit();
    }
    return file;
  }

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    return new BufferedOutputStream(_makeOutputStream(filename));
  }

  @Override
  public WritableByteChannel makeOutputChannel(String fileName) throws IOException
  {
    return Channels.newChannel(_makeOutputStream(fileName));
  }

  private FileOutputStream _makeOutputStream(String filename) throws IOException
  {
    File file = createdFiles.get(filename);
    if (file == null) {
      createdFiles.put(filename, file = new File(base, filename));
      file.deleteOnExit();
    } else if (!allowOverwrite) {
      throw new IOException("tmp file conflicts, file[" + filename + "] already exist!");
    }
    return new FileOutputStream(file);
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    final File file = createdFiles.get(filename);
    return file == null ? null : new FileInputStream(file);
  }

  @Override
  public void close() throws IOException
  {
    for (File file : createdFiles.values()) {
      FileUtils.deleteQuietly(file);
    }
    createdFiles.clear();
  }
}
