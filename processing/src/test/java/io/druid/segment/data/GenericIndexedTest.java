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

package io.druid.segment.data;

import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 */
public class GenericIndexedTest
{
  private static GenericIndexed<String> of(List<String> values)
  {
    return of(values.toArray(new String[0]));
  }

  private static GenericIndexed<String> of(String... values)
  {
    return GenericIndexed.v2(Arrays.asList(values), ObjectStrategy.STRING_STRATEGY);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNotSortedNoIndexOf() throws Exception
  {
    GenericIndexed<String> indexed = of("a", "c", "b");
    Assert.assertFalse(indexed.isSorted());
    indexed.indexOf("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSerializationNotSortedNoIndexOf() throws Exception
  {
    GenericIndexed<String> indexed = serializeAndDeserialize(of("a", "c", "b"));
    Assert.assertFalse(indexed.isSorted());
    indexed.indexOf("a");
  }

  @Test
  public void testSanity() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};
    GenericIndexed<String> indexed = of(strings);

    checkBasicAPIs(strings, indexed, true);

    Assert.assertEquals(-13, indexed.indexOf("q"));
    Assert.assertEquals(-9, indexed.indexOf("howdydo"));
    Assert.assertEquals(-1, indexed.indexOf("1111"));
  }

  @Test
  public void testSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};

    GenericIndexed<String> deserialized = serializeAndDeserialize(of(strings));

    checkBasicAPIs(strings, deserialized, true);

    Assert.assertEquals(-13, deserialized.indexOf("q"));
    Assert.assertEquals(-9, deserialized.indexOf("howdydo"));
    Assert.assertEquals(-1, deserialized.indexOf("1111"));
  }

  @Test
  public void testNotSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "k", "j", "l"};

    GenericIndexed<String> deserialized = serializeAndDeserialize(of(strings));
    checkBasicAPIs(strings, deserialized, false);
  }

  @Test
  public void testEmptySerialization() throws Exception
  {
    GenericIndexed<String> empty = of();
    Assert.assertEquals(0, empty.size());

    GenericIndexed<String> deserialized = serializeAndDeserialize(empty);
    Assert.assertEquals(0, deserialized.size());
  }

  private void checkBasicAPIs(String[] strings, GenericIndexed<String> index, boolean allowReverseLookup)
  {
    Assert.assertEquals(allowReverseLookup, index.isSorted());
    Assert.assertEquals(strings.length, index.size());
    for (int i = 0; i < strings.length; i++) {
      Assert.assertEquals(strings[i], index.get(i));
    }

    if (allowReverseLookup) {
      HashMap<String, Integer> mixedUp = Maps.newHashMap();
      for (int i = 0; i < strings.length; i++) {
        mixedUp.put(strings[i], i);
      }
      for (Map.Entry<String, Integer> entry : mixedUp.entrySet()) {
        Assert.assertEquals(entry.getValue().intValue(), index.indexOf(entry.getKey()));
      }
    } else {
      try {
        index.indexOf("xxx");
        Assert.fail("should throw exception");
      }
      catch (UnsupportedOperationException e) {
        // not supported
      }
    }
  }

  @Test
  public void testBulkSearch() throws Exception
  {
    List<String> values = IntStream.range(200, 300)
                               .filter(v -> v % 2 == 0)
                               .mapToObj(String::valueOf).collect(Collectors.toList());

    GenericIndexed<String> indexed1 = of(values);
    GenericIndexed<String> indexed2 = of(GuavaUtils.concat("", values));

    assertEquals(IntStream.empty(), indexed1, indexed2, IntStream.range(180, 200));
    assertEquals(IntStream.empty(), indexed1, indexed2, IntStream.range(300, 320));

    assertEquals(IntStream.range(0, 50), indexed1, indexed2, IntStream.range(200, 300));
    assertEquals(IntStream.range(0, 50), indexed1, indexed2, IntStream.range(180, 320));

    assertEquals(IntStream.range(0, 10), indexed1, indexed2, IntStream.range(180, 220));
    assertEquals(IntStream.range(40, 50), indexed1, indexed2, IntStream.range(280, 320));

    assertEquals(IntStream.of(49), indexed1, indexed2, IntStream.concat(IntStream.range(190, 200), IntStream.of(297, 298)));
    assertEquals(IntStream.of(48, 49), indexed1, indexed2, IntStream.concat(IntStream.range(190, 200), IntStream.of(296, 298)));

    assertEquals(IntStream.of(47, 48), indexed1, indexed2, IntStream.concat(IntStream.range(190, 200), IntStream.of(294, 296)));
  }

  private static void assertEquals(
      IntStream expected,
      GenericIndexed<String> indexed1,
      GenericIndexed<String> indexed2,
      IntStream values
  )
  {
    int[] e = expected.toArray();

    List<String> search = values.mapToObj(String::valueOf).collect(Collectors.toList());
    int[] r1 = indexed1.indexOf(search).toArray();
    int[] r2 = indexed2.indexOf(search).toArray();

    for (int i = 0; i < Math.min(e.length, r1.length); i++) {
      Assert.assertEquals(e[i], r1[i]);
    }
    Assert.assertEquals(e.length, r1.length);
    for (int i = 0; i < Math.min(e.length, r2.length); i++) {
      Assert.assertEquals(e[i] + 1, r2[i]);
    }
    Assert.assertEquals(e.length, r2.length);

    search = GuavaUtils.concat("", search);
    r1 = indexed1.indexOf(search).toArray();
    r2 = indexed2.indexOf(search).toArray();

    for (int i = 0; i < Math.min(e.length, r1.length); i++) {
      Assert.assertEquals(e[i], r1[i]);
    }
    Assert.assertEquals(e.length, r1.length);

    Assert.assertEquals(0, r2[0]);
    for (int i = 0; i < Math.min(e.length, r2.length); i++) {
      Assert.assertEquals(e[i] + 1, r2[i + 1]);
    }
    Assert.assertEquals(e.length + 1, r2.length);
  }

  private GenericIndexed<String> serializeAndDeserialize(GenericIndexed<String> indexed) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    indexed.writeToChannel(channel);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(indexed.getSerializedSize(), byteBuffer.remaining());
    GenericIndexed<String> deserialized = GenericIndexed.read(
        byteBuffer, ObjectStrategy.STRING_STRATEGY
    );
    Assert.assertEquals(0, byteBuffer.remaining());
    return deserialized;
  }
}
