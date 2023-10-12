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

package io.druid.segment.data;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class GenericIndexedWriterTest
{
  @Test
  public void test() throws IOException
  {
    IOPeon ioPeon = IOPeon.forTest();
    test(GenericIndexedWriter.forDictionaryV2(ioPeon, "test"));
    test(GenericIndexedWriter.forDictionaryV2(ioPeon, "test"));
  }

  private void test(GenericIndexedWriter<String> writer) throws IOException
  {
    writer.open();
    writer.add("a");
    writer.add("b");
    writer.add("c");
    writer.close();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (WritableByteChannel channel = Channels.newChannel(out)) {
      writer.writeToChannel(channel);
    }
    Assert.assertEquals(writer.getSerializedSize(), out.size());

    GenericIndexed<String> indexed = GenericIndexed.read(ByteBuffer.wrap(out.toByteArray()), ObjectStrategy.STRING_STRATEGY);
    Assert.assertEquals(3, indexed.size());
    Assert.assertEquals("a", indexed.get(0));
    Assert.assertEquals("b", indexed.get(1));
    Assert.assertEquals("c", indexed.get(2));
    Assert.assertEquals(0, indexed.indexOf("a"));
    Assert.assertEquals(1, indexed.indexOf("b"));
    Assert.assertEquals(2, indexed.indexOf("c"));
    Assert.assertTrue(indexed.indexOf("d") < 0);
  }
}
