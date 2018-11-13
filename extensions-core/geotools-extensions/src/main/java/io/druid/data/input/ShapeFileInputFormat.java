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

package io.druid.data.input;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.FileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ShapeFileInputFormat extends InputFormat<Void, Map<String, Object>>
{
  private static final Logger LOG = new Logger(ShapeFileInputFormat.class);

  private static final String ENCODING = "shape.encoding";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    List<InputSplit> splits = Lists.newArrayList();
    for (Path path : FileInputFormat.getInputPaths(context)) {
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      if (!fs.getFileStatus(path).isDirectory()) {
        throw new IllegalArgumentException("shape formatter accepts directory but '" + path + "' is not");
      }

      FileStatus[] files = fs.listStatus(path);
      FileStatus projectFile = Preconditions.checkNotNull(findFile(files, ".prj"), "Failed to find .prj");
      FileStatus shapeFile = Preconditions.checkNotNull(findFile(files, ".shp"), "Failed to find .shp");
      FileStatus shapeIndexFile = Preconditions.checkNotNull(findFile(files, ".shx"), "Failed to find .shx");
      FileStatus attributeFile = Preconditions.checkNotNull(findFile(files, ".dbf"), "Failed to find .dbf");
      splits.add(new ShapeSplit(
          projectFile.getPath().toString(),
          shapeFile.getPath().toString(),
          shapeIndexFile.getPath().toString(),
          attributeFile.getPath().toString(),
          projectFile.getLen() + shapeFile.getLen() + shapeIndexFile.getLen() + attributeFile.getLen()
      ));
    }
    for (int i = 0; i < splits.size(); i++) {
      LOG.info("Split-[%04d] : [%s]", i, splits.get(i));
    }

    return splits;
  }

  private FileStatus findFile(FileStatus[] files, String postFix)
  {
    for (FileStatus status : files) {
      if (status.getPath().getName().endsWith(postFix)) {
        return status;
      }
    }
    return null;
  }

  @Override
  public RecordReader<Void, Map<String, Object>> createRecordReader(InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    LOG.info("Reading %s", split);

    final ShapeSplit shapeSplit = (ShapeSplit) split;
    final Path shapeFile = new Path(shapeSplit.shapeFile);
    final Path shapeIndexFile = new Path(shapeSplit.shapeIndexFile);
    final Path attributeFile = new Path(shapeSplit.attributeFile);

    final Charset charset = Charset.forName(context.getConfiguration().get(ENCODING, StringUtils.UTF8_STRING));
    final GeometryFactory geometryFactory = new GeometryFactory();

    final ShapefileReader shapeReader = new ShapefileReader(new ShpFiles(File.createTempFile("druid_shape", ".shp"))
    {
      @Override
      public ReadableByteChannel getReadChannel(ShpFileType type, FileReader requestor) throws IOException
      {
        Configuration configuration = context.getConfiguration();
        if (type == ShpFileType.SHP) {
          return Channels.newChannel(shapeFile.getFileSystem(configuration).open(shapeFile));
        } else if (type == ShpFileType.SHX) {
          return Channels.newChannel(shapeFile.getFileSystem(configuration).open(shapeIndexFile));
        } else {
          throw new IllegalArgumentException("Not supported type " + type);
        }
      }
    }, true, false, geometryFactory);
    final DbaseFileReader attrReader = new DbaseFileReader(new ShpFiles(File.createTempFile("druid_shape", ".dbf"))
    {
      @Override
      public ReadableByteChannel getReadChannel(ShpFileType type, FileReader requestor) throws IOException
      {
        Configuration configuration = context.getConfiguration();
        if (type == ShpFileType.DBF) {
          return Channels.newChannel(attributeFile.getFileSystem(configuration).open(attributeFile));
        } else {
          throw new IllegalArgumentException("Not supported type " + type);
        }
      }
    }, false, charset);

    Path projectFile = new Path(shapeSplit.projectFile);
    StringWriter writer = new StringWriter();
    IOUtils.copy(projectFile.getFileSystem(context.getConfiguration()).open(projectFile), writer, charset);

    DbaseFileHeader header = attrReader.getHeader();
    final String[] fields = new String[header.getNumFields()];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = header.getFieldName(i);
    }
    final int numRecords = header.getNumRecords();
    final MathTransform transform = makeTransformer(writer.toString());
    LOG.info("Fields %s, numRecords %d", Arrays.toString(fields), numRecords);

    return new RecordReader<Void, Map<String, Object>>()
    {
      private int index;

      @Override
      public void initialize(InputSplit split, final TaskAttemptContext context)
          throws IOException, InterruptedException
      {}

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        return shapeReader.hasNext() && attrReader.hasNext();
      }

      @Override
      public Void getCurrentKey() throws IOException, InterruptedException
      {
        return null;
      }

      @Override
      public Map<String, Object> getCurrentValue() throws IOException, InterruptedException
      {
        Map<String, Object> row = Maps.newLinkedHashMap();
        Object[] values = attrReader.readEntry();
        for (int i = 0; i < values.length; i++) {
          row.put(fields[i], values[i]);
        }
        Geometry shape = (Geometry) shapeReader.nextRecord().shape();
        if (transform != null) {
          try {
            shape = JTS.transform(shape, transform);
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
        row.put("__geometry", shape.toText());
        index++;
        return row;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        return index / (float) numRecords;
      }

      @Override
      public void close() throws IOException
      {
        if (shapeReader != null) {
          shapeReader.close();
        }
        if (attrReader != null) {
          attrReader.close();
        }
      }
    };
  }

  // epsg-hsql seemed not working on MR
  static MathTransform makeTransformer(String projection)
  {
    try {
      final CoordinateReferenceSystem sourceCRS = CRS.parseWKT(projection);
      final CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");
      LOG.info(".. converting %s to EPSG:4326", sourceCRS.getCoordinateSystem().getName());
      return CRS.findMathTransform(sourceCRS, targetCRS, true);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static class ShapeSplit extends InputSplit implements Writable
  {
    private String projectFile;
    private String shapeFile;
    private String shapeIndexFile;
    private String attributeFile;
    private long length;

    public ShapeSplit()
    {
    }

    public ShapeSplit(String projectFile, String shapeFile, String shapeIndexFile, String attributeFile, long length)
    {
      this.projectFile = projectFile;
      this.shapeFile = shapeFile;
      this.shapeIndexFile = shapeIndexFile;
      this.attributeFile = attributeFile;
      this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      Text.writeString(out, projectFile);
      Text.writeString(out, shapeFile);
      Text.writeString(out, shapeIndexFile);
      Text.writeString(out, attributeFile);
      out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      projectFile = Text.readString(in);
      shapeFile = Text.readString(in);
      shapeIndexFile = Text.readString(in);
      attributeFile = Text.readString(in);
      length = in.readLong();
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return new String[0];
    }

    @Override
    public String toString()
    {
      return "ShapeSplit{" +
             "projectFile='" + projectFile + '\'' +
             ", shapeFile='" + shapeFile + '\'' +
             ", shapeIndexFile='" + shapeIndexFile + '\'' +
             ", attributeFile='" + attributeFile + '\'' +
             ", length=" + length +
             '}';
    }
  }
}
