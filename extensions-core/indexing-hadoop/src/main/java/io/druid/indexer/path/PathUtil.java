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

package io.druid.indexer.path;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class PathUtil
{
  public static List<String> resolve(Path basePath, List<String> paths, boolean recursive) throws IOException
  {
    List<String> resolved = Lists.newArrayList();
    Configuration configuration = new Configuration();
    if (basePath == null) {
      for (String path : paths) {
        Path child = new Path(path);
        FileSystem fileSystem = child.getFileSystem(configuration);
        if (isGlobPath(child)) {
          for (FileStatus status : fileSystem.globStatus(child)) {
            resolved.add(status.getPath().toString());
          }
        } else {
          RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(basePath, recursive);
          while (files.hasNext()) {
            resolved.add(files.next().getPath().toString());
          }
        }
      }
    } else {
      List<Path> targetPaths;
      if (GuavaUtils.isNullOrEmpty(paths)) {
        targetPaths = Arrays.asList(basePath);
      } else {
        targetPaths = Lists.newArrayList(Iterables.transform(paths, p -> new Path(basePath, p)));
      }
      FileSystem fileSystem = basePath.getFileSystem(configuration);
      for (Path child : targetPaths) {
        if (isGlobPath(child)) {
          for (FileStatus status : fileSystem.globStatus(child)) {
            resolved.add(relativize(basePath, status.getPath()));
          }
        } else {
          RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(child, recursive);
          while (files.hasNext()) {
            resolved.add(relativize(basePath, files.next().getPath()));
          }
        }
      }
    }
    return resolved;
  }

  public static String relativize(Path path1, Path path2)
  {
    final URI relativized = path1.toUri().relativize(path2.toUri());
    if (relativized == path2.toUri()) {
      return relativized.toString();
    } else {
      return relativized.getPath();
    }
  }

  private static boolean isGlobPath(Path path)
  {
    final String p = path.toUri().getPath();
    return p.indexOf('*') >= 0 || p.indexOf('?') >= 0 || p.indexOf('[') >= 0 || p.indexOf('{') >= 0;
  }
}
