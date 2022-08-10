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

package io.druid.data.input;

import io.druid.common.utils.StringUtils;
import io.druid.data.VLongUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BytesOutputStreamTest
{
  @Test
  public void testVarSizeString()
  {
    String[] tests = new String[6];
    tests[0] = "navis";
    for (int i = 1 ; i < tests.length; i++) {
      StringBuilder b = new StringBuilder();
      for (int j = 0; j < 16; j++) {
        b.append(tests[i - 1]);
      }
      tests[i] = b.toString();
    }
    BytesOutputStream out = new BytesOutputStream();
    for (int i = 0; i < tests.length; i++) {
      out.writeVarSizeBytes(StringUtils.toUtf8(tests[i]));
    }
    BytesInputStream in = new BytesInputStream(out.toByteArray());
    for (int i = 0; i < tests.length; i++) {
      Assert.assertEquals(tests[i], in.readVarSizeUTF());
    }

    // 128, 256, 65536, 16777216
    out.clear();
    out.writeVarInt(255);
    out.writeVarInt(65535);
    Assert.assertEquals(5, out.size());
    in = new BytesInputStream(out.toByteArray());
    Assert.assertEquals(255, in.readVarInt());
    Assert.assertEquals(65535, in.readVarInt());
    ByteBuffer buffer = out.asByteBuffer();

    Assert.assertEquals(2, VLongUtils.sizeOfUnsignedVarInt(255));
    Assert.assertEquals(3, VLongUtils.sizeOfUnsignedVarInt(65535));

    // 128, 16384, 2097152, 268435456
    out.clear();
    out.writeUnsignedVarInt(255);
    out.writeUnsignedVarInt(65535);
    Assert.assertEquals(5, out.size());
    in = new BytesInputStream(out.toByteArray());
    Assert.assertEquals(255, in.readUnsignedVarInt());
    Assert.assertEquals(65535, in.readUnsignedVarInt());
    Assert.assertEquals(255, VLongUtils.readUnsignedVarInt(buffer));
    Assert.assertEquals(65535, VLongUtils.readUnsignedVarInt(buffer));

    out.clear();
    out.writeShort(65535);
    in = new BytesInputStream(out.toByteArray());
    Assert.assertEquals(65535, in.readUnsignedShort());
  }
}
