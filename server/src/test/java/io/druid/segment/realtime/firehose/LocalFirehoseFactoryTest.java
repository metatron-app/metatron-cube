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

package io.druid.segment.realtime.firehose;

import io.druid.common.Progressing;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.segment.TestIndex;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class LocalFirehoseFactoryTest
{
  @Test
  public void test() throws Exception
  {
    final URL resource = LocalFirehoseFactory.class.getClassLoader().getResource("druid.sample.tsv.bottom");
    if (resource == null) {
      throw new IllegalArgumentException();
    }
    File parent = new File(resource.getPath()).getParentFile();
    LocalFirehoseFactory factory = new LocalFirehoseFactory(parent, "druid*", null, false, false, TestIndex.PARSER);
    Firehose firehose = factory.connect(TestIndex.PARSER);
    while (firehose.hasMore()) {
      InputRow row = firehose.nextRow();
      Assert.assertNotNull(row);
      Assert.assertTrue(((Progressing) firehose).progress() > 0);
    }
    firehose.close();
  }
}