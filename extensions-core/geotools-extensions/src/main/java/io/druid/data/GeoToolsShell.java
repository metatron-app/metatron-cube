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

package io.druid.data;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.cli.shell.CommonShell;
import io.druid.data.input.ShapeFileInputFormat;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.FileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.referencing.CRS;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.List;

public class GeoToolsShell extends CommonShell.WithUtils
{
  @Override
  public void run(List<String> arguments) throws Exception
  {
    if (arguments.isEmpty()) {
      return;
    }
    try (Terminal terminal = TerminalBuilder.builder().build()) {
      execute(terminal, arguments);
    }
  }

  private void execute(Terminal terminal, List<String> arguments) throws IOException
  {
    final PrintWriter writer = terminal.writer();
    if (arguments.get(0).equals("columns")) {
      if (arguments.size() < 2) {
        log(writer, "!! missing <shape-directory> [charset]");
        return;
      }
      final Configuration configuration = new Configuration();
      final Charset charset = arguments.size() > 2 ? Charset.forName(arguments.get(2)) : Charset.defaultCharset();

      Path path = new Path(arguments.get(1));
      FileSystem fs = path.getFileSystem(configuration);

      FileStatus[] files = fs.listStatus(path);

      FileStatus prj = ShapeFileInputFormat.findFile(files, ".prj");
      if (prj != null) {
        try {
          Path projectFile = prj.getPath();
          StringWriter projection = new StringWriter();
          IOUtils.copy(projectFile.getFileSystem(configuration).open(projectFile), projection, charset);
          CoordinateReferenceSystem sourceCRS = CRS.parseWKT(projection.toString());
          log(writer, "CRS = %s", sourceCRS.getCoordinateSystem().getName());
        }
        catch (Exception e) {
          log(writer, "CRS ???");
        }
      }

      FileStatus dbf = ShapeFileInputFormat.findFile(files, ".dbf");
      if (dbf == null) {
        log(writer, "!! Failed to find .dbf in %s", arguments.get(1));
        return;
      }
      final Path attributeFile = dbf.getPath();
      DbaseFileReader attrReader = new DbaseFileReader(new ShpFiles(File.createTempFile("druid_shape", ".dbf"))
      {
        @Override
        public ReadableByteChannel getReadChannel(ShpFileType type, FileReader requestor) throws IOException
        {
          Preconditions.checkArgument(type == ShpFileType.DBF, "Not supported type %s", type);
          return Channels.newChannel(attributeFile.getFileSystem(configuration).open(attributeFile));
        }
      }, false, charset);

      DbaseFileHeader header = attrReader.getHeader();
      List<String> fieldNames = Lists.newArrayList();
      List<String> fieldTypes = Lists.newArrayList();
      List<String> dimensions = Lists.newArrayList();
      List<String> metrics = Lists.newArrayList();
      for (int i = 0; i < header.getNumFields(); i++) {
        String fieldName = header.getFieldName(i);
        Class fieldClass = header.getFieldClass(i);
        ValueType fieldType = ValueType.of(fieldClass);
        fieldNames.add(String.format("\"%s\"", fieldName));
        fieldTypes.add(String.format("\"%s\"", fieldClass.getSimpleName()));
        if (fieldClass == String.class) {
          dimensions.add(String.format("\"%s\"", fieldName));
        } else if (fieldType.isPrimitive()) {
          metrics.add(String.format("{\"name\": \"%s\", \"typeName\": \"%s\"}", fieldName, fieldType));
        }
      }
      int numRecords = header.getNumRecords();
      log(writer, "Field Names = %s", fieldNames);
      log(writer, "Field Types = %s", fieldTypes);
      log(writer, "Num Records = %,d", numRecords);
      log(writer, "Dimensions = %s", dimensions);
      log(writer, "Metrics = %s", Joiner.on(",\n     ").join(metrics));
      attrReader.close();
    }
  }

  private static void log(PrintWriter writer, String message, Object... arguments)
  {
    writer.println(StringUtils.safeFormat(message, arguments));
    writer.flush();
  }
}
