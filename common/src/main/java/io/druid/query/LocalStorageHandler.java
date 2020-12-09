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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.inject.Inject;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.PropUtils;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatters;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class LocalStorageHandler implements StorageHandler
{
  private static final Logger LOG = new Logger(LocalStorageHandler.class);

  private final ObjectMapper mapper;

  @Inject
  public LocalStorageHandler(ObjectMapper mapper) {this.mapper = mapper;}

  @Override
  public Sequence<Row> read(List<URI> locations, InputRowParser parser, Map<String, Object> context) throws IOException
  {
    // todo
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> write(URI location, QueryResult result, Map<String, Object> context)
      throws IOException
  {
    LOG.info("Result will be forwarded to [%s] with context %s", location, context);
    File targetDirectory = new File(location.getPath());
    boolean cleanup = PropUtils.parseBoolean(context, CLEANUP, false);
    if (cleanup) {
      FileUtils.deleteDirectory(targetDirectory);
    }
    if (targetDirectory.isFile()) {
      throw new IAE("target location [%s] should not be a file", location);
    }
    if (!targetDirectory.exists() && !targetDirectory.mkdirs()) {
      throw new IAE("failed to make target directory");
    }
    File dataFile = new File(targetDirectory, PropUtils.parseString(context, DATA_FILENAME, "data"));

    final CountingAccumulator exporter = toExporter(context, mapper, location, dataFile);
    if (exporter == null) {
      throw new IAE("Cannot find writer of format '%s'", Formatters.getFormat(context));
    }
    Map<String, Object> info = Maps.newLinkedHashMap();
    try {
      result.getSequence().accumulate(null, exporter.init());
    }
    finally {
      info.putAll(exporter.close());
    }
    return info;
  }

  CountingAccumulator toExporter(
      Map<String, Object> context,
      ObjectMapper mapper,
      final URI location,
      final File dataFile
  )
      throws IOException
  {
    return Formatters.toBasicExporter(
        context, mapper, new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return new FileOutputStream(dataFile);
          }

          @Override
          public String toString()
          {
            return rewrite(location, dataFile);
          }
        }
    );
  }

  private String rewrite(URI location, File path)
  {
    try {
      return new URI(
          location.getScheme(),
          location.getUserInfo(),
          location.getHost(),
          location.getPort(),
          path.getAbsolutePath(),
          null,
          null
      ).toString();
    }
    catch (URISyntaxException e) {
      return path.getAbsolutePath();
    }
  }
}
