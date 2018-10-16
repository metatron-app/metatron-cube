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

package io.druid.indexer.path;

import com.google.common.collect.Maps;
import io.druid.data.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

public class PartitionExtractorTest
{
  @Test
  public void test()
  {
    HadoopCombineInputFormat format = new HadoopCombineInputFormat();
    Map<String, String> expected = Maps.newHashMap();
    expected.put("yy", "2018");
    expected.put("MM", "10");
    expected.put("dd", "16");
    expected.put("HH", "10");
    expected.put("mm", "25");
    expected.put("ss", "100");

    Map<String, String> map = format.extractPartition(new Path(
        "hdfs://localhost:9000/some/path/yy=2018/MM=10/dd=16/HH=10/mm=25/ss=100"), null);
    Assert.assertEquals(expected, map);

    Pair<Matcher, Set<String>> extractor = format.makeExtractor(
        "/(?<yy>\\d{4})/(?<MM>\\d{2})/(?<dd>\\d{2})/(?<HH>\\d{2})/(?<mm>\\d{2})/(?<ss>\\d+)");
    map = format.extractPartition(new Path(
        "hdfs://localhost:9000/some/path/2018/10/16/10/25/100"), extractor);
    Assert.assertEquals(expected, map);
  }
}
