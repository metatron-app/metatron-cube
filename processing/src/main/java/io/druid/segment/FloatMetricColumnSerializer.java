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

package io.druid.segment;

import io.druid.common.utils.SerializerUtils;
import io.druid.segment.data.ColumnPartWriter.FloatType;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
@Deprecated
public class FloatMetricColumnSerializer extends MetricColumnSerializer.Deprecated
{
  private final String metricName;
  private final File outDir;

  private FloatType writer;

  public FloatMetricColumnSerializer(String metricName, File outDir)
  {
    this.metricName = metricName;
    this.outDir = outDir;
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    writer = FloatType.create(
        ioPeon, String.format("%s_little", metricName), IndexIO.BYTE_ORDER, IndexSpec.DEFAULT_COMPRESSION
    );
    writer.open();
  }

  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    float val = (obj == null) ? 0 : ((Number) obj).floatValue();
    writer.add(val);
  }

  @Override
  public void close() throws IOException
  {
    final File outFile = IndexIO.makeMetricFile(outDir, metricName, IndexIO.BYTE_ORDER);
    outFile.delete();
    writer.close();
    try (WritableByteChannel channel = new FileOutputStream(outFile, true).getChannel()) {
      channel.write(ByteBuffer.wrap(MetricHolder.version));
      SerializerUtils.writeString(channel, metricName);
      SerializerUtils.writeString(channel, "float");
      writer.writeToChannel(channel);
    }
    IndexIO.checkFileSize(outFile);

    writer = null;
  }
}
