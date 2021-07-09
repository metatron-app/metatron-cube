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

package io.druid.segment.lucene;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.collections.IntList;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.FST;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutomatonMatcherTest
{
  @Test
  public void test() throws IOException
  {
    Random r = new Random();
    StringBuilder sb = new StringBuilder();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < 500000; i++) {
      for (int j = 3 + r.nextInt(5); j >= 0; j--) {
        sb.append((char)('a' + r.nextInt('z' - 'a')));
      }
      values.add(sb.toString());
      sb.setLength(0);
    }
    Collections.sort(values);
    int x = 1;
    for (int i = 1; i < values.size(); i++) {
      if (!values.get(i).equals(values.get(i - 1))) {
        values.set(x++, values.get(i));
      }
    }
    FSTBuilder b = new FSTBuilder();
    for (int i = 0; i < x; i++) {
      b.addEntry(values.get(i), (long) i);
    }
    FST<Long> fst = b.done();

    System.out.println("[RegexpMatcherTest/test] " + x + " -> " + b.getNodeCount());

    long p = System.currentTimeMillis();
    IntList match1 = AutomatonMatcher.regexMatch("[ab].+[xyz]", fst);
    System.out.println("Took " + (System.currentTimeMillis() - p) + " msec");

    p = System.currentTimeMillis();
    IntList match2 = new IntList();
    Matcher matcher = Pattern.compile("[ab].+[xyz]").matcher("");
    for (int i = 0; i < x; i++) {
      if (matcher.reset(values.get(i)).matches()) {
        match2.add(i);
      }
    }
    System.out.println("Took " + (System.currentTimeMillis() - p) + " msec");
    Assert.assertTrue(Iterators.elementsEqual(match1.sort().iterator(), match2.iterator()));

    p = System.currentTimeMillis();
    Automaton automaton = PrefixQuery.toAutomaton(new BytesRef("abc".getBytes()));
    match1 = AutomatonMatcher.match(automaton, fst);
    System.out.println("Took " + (System.currentTimeMillis() - p) + " msec");

    match2.clear();
    p = System.currentTimeMillis();
    for (int i = 0; i < x; i++) {
      if (values.get(i).startsWith("abc")) {
        match2.add(i);
      }
    }
    System.out.println("Took " + (System.currentTimeMillis() - p) + " msec");
    Assert.assertTrue(Iterators.elementsEqual(match1.sort().iterator(), match2.iterator()));
  }
}
