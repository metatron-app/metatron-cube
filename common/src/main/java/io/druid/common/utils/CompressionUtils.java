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

package io.druid.common.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.metamx.common.logger.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 */
public class CompressionUtils extends com.metamx.common.CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);

  public static long store(File directory, OutputStream out, int bufferSize) throws IOException
  {
    final File[] files = directory.listFiles();
    if (files == null) {
      throw new IOException(String.format("directory[%s] is not a directory", directory));
    }

    long totalSize = 0;
    try (final ZipOutputStream zipOut = new ZipOutputStream(out)) {
      zipOut.setLevel(Deflater.NO_COMPRESSION);
      for (File file : files) {
        log.info("Adding file[%s] with size[%,d].  Total size so far[%,d]", file, file.length(), totalSize);
        if (file.length() >= Integer.MAX_VALUE) {
          zipOut.finish();
          throw new IOException(String.format("file[%s] too large [%,d]", file, file.length()));
        }
        zipOut.putNextEntry(new ZipEntry(file.getName()));
        totalSize += ByteStreams.copy(new BufferedInputStream(new FileInputStream(file), bufferSize), zipOut);
      }
      zipOut.closeEntry();
      // Workaround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
      zipOut.flush();
    }

    return totalSize;
  }
}
