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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.indexer.path.HynixPathSpec;
import io.druid.indexer.path.HynixPathSpecElement;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hive")
public class HivePathSpec extends HynixPathSpec
{
  private static final Logger logger = new Logger(HivePathSpec.class);

  @JsonCreator
  public HivePathSpec(
      @JsonProperty("source") String source,
      @JsonProperty("metastoreUri") String metastoreUri,
      @JsonProperty("partialPartitions") Map<String, String> partialPartitions
  ) throws Exception
  {
    super(extract(source, metastoreUri, partialPartitions));
  }

  private static HynixPathSpec extract(String source, String metastoreUri, Map<String, String> partialPartitions)
      throws Exception
  {
    String dbName;
    String tableName;
    int index = Preconditions.checkNotNull(source, "source cannot be null").indexOf('.');
    if (index < 0) {
      dbName = "default";
      tableName = source;
    } else {
      dbName = source.substring(0, index);
      tableName = source.substring(index + 1);
    }

    HiveConf conf = new HiveConf();
    if (metastoreUri != null && !metastoreUri.isEmpty()) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    }
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);

    try {
      Table table = new Table(client.getTable(dbName, tableName));
      logger.info(
          "Got table '%s.%s'. partitioned=%s, format=%s, serde=%s, location=%s",
          table.getDbName(),
          table.getTableName(),
          table.isPartitioned(),
          table.getInputFormatClass(),
          table.getSerializationLib(),
          table.getDataLocation()
      );
      // todo rewrite parser spec with serde

      Class inputFormat = table.getInputFormatClass();
      List<HynixPathSpecElement> pathSpecs = Lists.newArrayList();
      if (table.isPartitioned()) {
        List<String> partitionVals = Lists.newArrayList();
        if (partialPartitions != null && !partialPartitions.isEmpty()) {
          for (FieldSchema partitionKey : table.getPartitionKeys()) {
            String partitionVal = partialPartitions.remove(partitionKey.getName());
            if (partitionVal != null) {
              partitionVals.add(partitionVal);
              continue;
            }
            if (!partialPartitions.isEmpty()) {
              logger.warn("some partition values are not used.. %s" + partialPartitions);
            }
            break;
          }
        }
        for (org.apache.hadoop.hive.metastore.api.Partition partition :
            partitionVals.isEmpty() ?
            client.listPartitions(dbName, tableName, (short) -1) :
            client.listPartitions(dbName, tableName, partitionVals, (short) -1)) {
          pathSpecs.add(new HynixPathSpecElement(new Partition(table, partition).getLocation(), null, null, null));
        }
      } else {
        if (partialPartitions != null && !partialPartitions.isEmpty()) {
          logger.warn(
              "table '%s.%s' is not partitioned table.. ignoring partial partition %s",
              table.getDbName(),
              table.getTableName(),
              partialPartitions
          );
        }
        pathSpecs.add(new HynixPathSpecElement(table.getDataLocation().toString(), null, null, null));
      }
      return new HynixPathSpec(null, pathSpecs, inputFormat, null, false, table.isPartitioned(), null);
    }
    finally {
      client.close();
    }
  }
}
