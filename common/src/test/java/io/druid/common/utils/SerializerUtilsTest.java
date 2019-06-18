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

package io.druid.common.utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

public class SerializerUtilsTest
{
  private  final float delta = 0;
  private  final String [] strings = {"1#","2","3"};
  private  final int [] ints = {1,2,3};
  private  final float [] floats = {1.1f,2,3};
  private  final long [] longs = {3,2,1};
  private  final Charset UTF8 = Charset.forName("UTF-8");
  private  byte [] stringsByte;
  private  byte [] intsByte;
  private  byte [] floatsByte;
  private  byte [] longsByte;
  private ByteArrayOutputStream outStream;

  @Before
  public void setUpByteArrays() throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    out.writeInt(strings.length);
    for (int i = 0;i < strings.length;i++) {
      byte [] stringBytes = strings[i].getBytes(UTF8);
      out.writeInt(stringBytes.length);
      out.write(strings[i].getBytes());
    }
    out.close();
    stringsByte = bos.toByteArray();
    bos.close();
    bos = new ByteArrayOutputStream();
    out = new DataOutputStream(bos);
    out.writeInt(ints.length);
    for (int i = 0;i < ints.length;i++) {
      out.writeInt(ints[i]);
    }
    out.close();
    intsByte = bos.toByteArray();
    bos.close();
    bos = new ByteArrayOutputStream();
    out = new DataOutputStream(bos);
    out.writeInt(floats.length);
    for (int i = 0;i < ints.length;i++) {
      out.writeFloat(floats[i]);
    }
    out.close();
    floatsByte = bos.toByteArray();
    bos.close();
    bos = new ByteArrayOutputStream();
    out = new DataOutputStream(bos);
    out.writeInt(longs.length);
    for (int i = 0;i < longs.length;i++) {
      out.writeLong(longs[i]);
    }
    out.close();
    longsByte = bos.toByteArray();
    bos.close();
    outStream = new ByteArrayOutputStream();
  }

  @Test
  public void testChannelWritelong() throws IOException
  {
    final int index = 0;
    WritableByteChannel channelOutput = Channels.newChannel(outStream);
    SerializerUtils.writeLong(channelOutput, longs[index]);
    ByteArrayInputStream inputstream = new ByteArrayInputStream(outStream.toByteArray());
    channelOutput.close();
    inputstream.close();
    long expected = SerializerUtils.readLong(inputstream);
    long actuals = longs[index];
    Assert.assertEquals(expected, actuals);
  }

  @Test
  public void testChannelWriteString() throws IOException
  {
    final int index = 0; 
    WritableByteChannel channelOutput = Channels.newChannel(outStream);
    SerializerUtils.writeString(channelOutput, strings[index]);
    ByteArrayInputStream inputstream = new ByteArrayInputStream(outStream.toByteArray());
    channelOutput.close();
    inputstream.close();
    String expected = SerializerUtils.readString(inputstream);
    String actuals = strings[index];
    Assert.assertEquals(expected, actuals);
  }

  @After
  public void tearDown() throws IOException
  {
    outStream.close();
  }
}
