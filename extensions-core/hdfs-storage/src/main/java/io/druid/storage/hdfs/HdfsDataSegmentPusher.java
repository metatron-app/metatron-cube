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

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.PropUtils;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatters;
import io.druid.data.output.formatter.OrcFormatter;
import io.druid.query.ResultWriter;
import io.druid.query.TabularFormat;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher, ResultWriter
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      Configuration hadoopConfig,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;

    log.info("Configured HDFS as deep storage");
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return new Path(config.getStorageDirectory()).toUri().toString();
  }

  @Override
  public DataSegment push(File inDir, DataSegment segment) throws IOException
  {
    final String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);

    log.info(
        "Copying segment[%s] to HDFS at location[%s/%s]",
        segment.getIdentifier(),
        config.getStorageDirectory(),
        storageDir
    );

    Path outFile = new Path(String.format("%s/%s/index.zip", config.getStorageDirectory(), storageDir));
    FileSystem fs = outFile.getFileSystem(hadoopConfig);

    fs.mkdirs(outFile.getParent());
    log.info("Compressing files from[%s] to [%s]", inDir, outFile);

    final long size;
    try (FSDataOutputStream out = fs.create(outFile)) {
      size = CompressionUtils.zip(inDir, out);
    }

    return createDescriptorFile(
        segment.withLoadSpec(makeLoadSpec(outFile))
               .withSize(size)
               .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir)),
        outFile.getParent(),
        fs
    );
  }

  private DataSegment createDescriptorFile(DataSegment segment, Path outDir, final FileSystem fs) throws IOException
  {
    final Path descriptorFile = new Path(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    ByteSource
        .wrap(jsonMapper.writeValueAsBytes(segment))
        .copyTo(new HdfsOutputStreamSupplier(fs, descriptorFile));
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(Path outFile)
  {
    return ImmutableMap.<String, Object>of("type", "hdfs", "path", outFile.toString());
  }

  @Override
  public Map<String, Object> write(URI location, TabularFormat result, Map<String, Object> context)
      throws IOException
  {
    log.info("Writing to " + location + " with context " + context);
    Path parent = new Path(location);
    Path targetDirectory = toHadoopPath(location);
    FileSystem fileSystem = targetDirectory.getFileSystem(hadoopConfig);

    boolean cleanup = PropUtils.parseBoolean(context, "cleanup", false);
    if (cleanup) {
      fileSystem.delete(targetDirectory, true);
    }
    if (fileSystem.isFile(targetDirectory)) {
      throw new IllegalStateException("'resultDirectory' should not be a file");
    }
    if (!fileSystem.exists(targetDirectory) && !fileSystem.mkdirs(targetDirectory)) {
      throw new IllegalStateException("failed to make target directory");
    }
    String fileName = PropUtils.parseString(context, "dataFileName", null);
    Path dataFile = new Path(targetDirectory, Strings.isNullOrEmpty(fileName) ? "data" : fileName);

    Map<String, Object> info = Maps.newHashMap();
    try (CountingAccumulator accumulator = toExporter(context, jsonMapper, fileSystem, dataFile)) {
      accumulator.init();
      result.getSequence().accumulate(null, accumulator);
      info.put("numRows", accumulator.count());
    }
    Path dataLocation = new Path(parent, dataFile.getName());
    info.put("data", ImmutableMap.of(dataLocation.toString(), fileSystem.getFileStatus(dataFile).getLen()));

    Map<String, Object> metaData = result.getMetaData();
    if (metaData != null && !metaData.isEmpty()) {
      Path metaFile = new Path(targetDirectory, ".meta");
      try (OutputStream output = fileSystem.create(metaFile)) {
        jsonMapper.writeValue(output, metaData);
      }
      Path metaLocation = new Path(parent, metaFile.getName());
      info.put("meta", ImmutableMap.of(metaLocation.toString(), fileSystem.getFileStatus(metaFile).getLen()));
    }
    return info;
  }

  private Path toHadoopPath(URI uri)
  {
    return uri.getScheme().equals(ResultWriter.FILE_SCHEME) ? new Path(removeHostPort(uri)) : new Path(uri);
  }

  private URI removeHostPort(URI uri)
  {
    try {
      return new URI(uri.getScheme(), uri.getUserInfo(), null, -1, uri.getPath(), uri.getQuery(), uri.getFragment());
    }
    catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

  private CountingAccumulator toExporter(Map<String, Object> context, ObjectMapper mapper, FileSystem fs, Path dataFile)
      throws IOException
  {
    String format = Objects.toString(context.get("format"), null);
    if (format.equals("orc")) {
      return Formatters.wrapToExporter(new OrcFormatter(dataFile, Objects.toString(context.get("schema"), null)));
    }
    return Formatters.toBasicExporter(context, mapper, new HdfsOutputStreamSupplier(fs, dataFile));
  }

  private static class HdfsOutputStreamSupplier extends ByteSink
  {
    private final FileSystem fs;
    private final Path descriptorFile;

    public HdfsOutputStreamSupplier(FileSystem fs, Path descriptorFile)
    {
      this.fs = fs;
      this.descriptorFile = descriptorFile;
    }

    @Override
    public OutputStream openStream() throws IOException
    {
      return fs.create(descriptorFile);
    }
  }
}
