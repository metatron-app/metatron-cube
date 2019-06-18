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

package io.druid.hive;

import com.metamx.common.logger.Logger;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Map;

/**
 */
public class DruidHiveStorageHandler extends DefaultStorageHandler implements InputEstimator
{
  private static final Logger logger = new Logger(DruidHiveStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass()
  {
    return DruidHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass()
  {
    return DruidHiveInputFormat.class;
  }

  @Override
  public Class getSerDeClass()
  {
    return DruidHiveSerDe.class;
  }

  @Override
  public InputEstimator.Estimation estimate(JobConf job, TableScanOperator ts, long remaining) throws HiveException
  {
    TableScanDesc desc = ts.getConf();
    Table table = desc.getTableMetadata();
    logger.info("Estimating size of %s.%s", table.getDbName(), table.getTableName());

    if (table.getDataLocation() == null) {
      return new InputEstimator.Estimation(1, Integer.MAX_VALUE);
    }

    JobConf dummy = new JobConf(job);
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      dummy.set(entry.getKey(), entry.getValue());
    }
    if (desc.getFilterExpr() != null) {
      dummy.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(desc.getFilterExpr()));
    }
    dummy.set(FileInputFormat.INPUT_DIR, table.getDataLocation().toString());

    DruidHiveInputFormat formatter = new DruidHiveInputFormat();
    try {
      long totalLength = 0;
      for (InputSplit split : formatter.getSplits(dummy, -1)) {
        totalLength += ((FileSplit) split).getLength();
      }
      logger.info("Estimated size %d bytes", totalLength);
      return new InputEstimator.Estimation(1, totalLength);
    }
    catch (IOException e) {
      logger.warn(e, "Failed to estimate size of %s.%s", table.getDbName(), table.getTableName());
    }
    return new InputEstimator.Estimation(1, Integer.MAX_VALUE);
  }
}
