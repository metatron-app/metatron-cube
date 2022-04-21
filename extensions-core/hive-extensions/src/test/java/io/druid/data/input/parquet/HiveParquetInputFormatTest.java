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
package io.druid.data.input.parquet;

import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.segment.indexing.DataSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HiveParquetInputFormatTest
{
  @Test
  public void test() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/wikipedia_hadoop_parquet_job.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia_list.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    @SuppressWarnings("unchecked")
    InputFormat<?, Map<String, Object>> inputFormat = ReflectionUtils.newInstance(
        HiveParquetInputFormat.class, job.getConfiguration()
    );

    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader<?, Map<String, Object>> reader = inputFormat.createRecordReader(split, context);
    reader.initialize(split, context);
    reader.nextKeyValue();

    Map<String, Object> data = reader.getCurrentValue();
    assertEquals(7, data.size());

    // field not read, should return null
    assertNull(data.get("added"));

    assertEquals(200L, data.get("deleted"));
    assertEquals(-143L, data.get("delta"));
    assertEquals(Arrays.asList("en", "zh"), data.get("language"));
    assertEquals("Gypsy Danger", data.get("page"));
    assertEquals("2013-08-31T01:02:33Z", data.get("timestamp"));
    assertEquals("true", data.get("unpatrolled"));
    assertEquals("nuclear", data.get("user"));

    reader.close();

    // explicit required columns
    job.getConfiguration().set(DataSchema.REQUIRED_COLUMNS, "not-exists,deleted,language,page");

    reader = inputFormat.createRecordReader(split, context);
    reader.initialize(split, context);
    reader.nextKeyValue();

    data = reader.getCurrentValue();
    assertEquals(3, data.size());

    reader.close();
  }
}
