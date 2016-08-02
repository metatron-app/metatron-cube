/*
 * Copyright 2011,2012 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class DelimitedParserTest
{
  @Test
  public void testBasic()
  {
    DelimitedParser delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.<String>absent(),
        Arrays.asList("x", "y", "z"),
        null
    );
    String body = "hello\tworld\tfoo";
    Map jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("x", "hello", "y", "world", "z", "foo"),
        jsonMap
    );
  }

  @Test
  public void testMoreKeys()
  {
    DelimitedParser delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.<String>absent(),
        Arrays.asList("x", "y", "z", "w"),
        null
    );
    String body = "hello\tworld\tfoo";
    Map jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("x", "hello", "y", "world", "z", "foo", "w", ""),
        jsonMap
    );
  }

  @Test
  public void testMoreValues()
  {
    DelimitedParser delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.<String>absent(),
        Arrays.asList("x", "y", "z"),
        null
    );
    String body = "hello\tworld\tfoo\txxx";
    Map jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("x", "hello", "y", "world", "z", "foo"),
        jsonMap
    );
  }

  @Test
  public void testListParsing()
  {
    DelimitedParser delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.of(":"),
        Arrays.asList("x", "y"),
        null
    );
    String body = "hello:world\tfoo:xxx";
    Map jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("x", Arrays.asList("hello", "world"), "y", Arrays.asList("foo", "xxx")),
        jsonMap
    );
  }

  @Test
  public void testSelectiveListParsing()
  {
    DelimitedParser delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.of(":"),
        Arrays.asList("x", "y"),
        Arrays.asList("y")
    );
    String body = "hello:world\tfoo:xxx";
    Map jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("x", "hello:world", "y", Arrays.asList("foo", "xxx")),
        jsonMap
    );
  }
}
