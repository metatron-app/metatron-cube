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

package io.druid.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.druid.indexer.hadoop.QueryBasedInputFormat;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.segment.filter.Filters;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DruidHiveInputFormat extends QueryBasedInputFormat implements HiveOutputFormat
{
  public static final String CONF_SELECT_COLUMNS = "hive.io.file.readcolumn.names";

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    DruidInputSplit[] splits = getInputSplits(job);

    String input = job.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, "");
    String[] dirs = org.apache.hadoop.util.StringUtils.split(input);
    if (dirs.length == 0) {
      throw new IllegalStateException("input dir is null");
    }
    Path path = new Path(dirs[0]);
    InputSplit[] converted = new InputSplit[splits.length];
    for (int i = 0; i < converted.length; i++) {
      converted[i] = new InputSplitWrapper(path, splits[i]);
    }
    return converted;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
  {
    job.set(CONF_DRUID_COLUMNS, job.get(CONF_SELECT_COLUMNS));
    return super.getRecordReader(((InputSplitWrapper) split).druidSplit, job, reporter);
  }

  @Override
  protected final Configuration configure(Configuration configuration, ObjectMapper mapper)
      throws IOException
  {
    ExprNodeGenericFuncDesc exprDesc = ExpressionConverter.deserializeExprDesc(configuration);
    String timeColumn = configuration.get(CONF_DRUID_TIME_COLUMN_NAME, EventHolder.timestampKey);
    logger.info("Using timestamp column %s", timeColumn);

    boolean revertCast = configuration.getBoolean(CONF_DRUID_REVERT_CAST, true);

    Map<String, TypeInfo> types = ExpressionConverter.getColumnTypes(configuration, timeColumn);
    Map<String, List<Range>> converted = ExpressionConverter.getRanges(exprDesc, types, revertCast);
    List<Range> timeRanges = converted.remove(timeColumn);
    if (timeRanges == null || timeRanges.isEmpty()) {
      logger.warn("Failed to extract intervals from predicate.. regarded as %s", DEFAULT_INTERVAL);
    } else {
      List<Interval> intervals = ExpressionConverter.toInterval(timeRanges);
      configuration.set(
          CONF_DRUID_INTERVALS,
          StringUtils.join( Lists.transform(intervals, Functions.toStringFunction()), ',')
      );
    }

    final boolean uppercase = configuration.getBoolean(CONF_DRUID_COLUMNS_UPPERCASE, false);

    List<DimFilter> filters = Lists.newArrayList();
    for (Map.Entry<String, List<Range>> entry : converted.entrySet()) {
      TypeInfo typeInfo = types.get(entry.getKey());
      if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
          ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        String dimension = uppercase ? entry.getKey().toUpperCase() : entry.getKey();
        DimFilter filter = Filters.toFilter(dimension, entry.getValue());
        if (filter != null) {
          filters.add(filter);
        }
      }
    }
    if (!filters.isEmpty()) {
      configuration.set(CONF_DRUID_FILTERS, mapper.writeValueAsString(new AndDimFilter(filters).optimize()));
    }
    return configuration;
  }

  @Override
  public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc,
      Path finalOutPath,
      Class valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress
  ) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  public static class InputSplitWrapper extends FileSplit
  {
    private final DruidInputSplit druidSplit;

    public InputSplitWrapper() {
      this.druidSplit = new DruidInputSplit();
    }

    public InputSplitWrapper(Path path, DruidInputSplit druidSplit)
    {
      super(path, 0, druidSplit.getLength(), druidSplit.getLocations());
      this.druidSplit = druidSplit;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      super.write(out);
      druidSplit.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      super.readFields(in);
      druidSplit.readFields(in);
    }
  }
}