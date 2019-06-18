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

package io.druid.query.aggregation.doccol;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DocumentsColumnTest {
  public DocumentsColumnTest()
  {

  }

  @Test
  public void testToBytes() {
    try {
      String doc = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("three.txt"), "UTF-8");
      String doc2 = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("monte.txt"), "UTF-8");

      DocumentsColumn documentsColumn = new DocumentsColumn(true);
      documentsColumn.add(doc).add(doc2);
      DocumentsColumn documentsColumn2 = new DocumentsColumn(false);
      documentsColumn2.add(doc).add(doc2);

      byte[] bytes = documentsColumn.toBytes();
      DocumentsColumn ret = DocumentsColumn.fromBytes(bytes);

      Assert.assertEquals(documentsColumn, ret);

      byte[] bytes2 = documentsColumn2.toBytes();
      DocumentsColumn ret2 = DocumentsColumn.fromBytes(bytes2);

      Assert.assertEquals(documentsColumn, ret2);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
