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

package io.druid.indexer;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Pattern;

public class HadoopSettlingMatcherFactory
{
  final private Map<String, Pattern> patternMap;

  public HadoopSettlingMatcherFactory()
  {
    patternMap = Maps.newHashMap();
  }

  public HadoopSettlingMatcher getSettlingMatcher(String[] patternStrings)
  {
    Pattern[] patterns = new Pattern[patternStrings.length];

    for (int idx = 0; idx < patternStrings.length; idx++) {
      Pattern pattern = patternMap.get(patternStrings[idx]);
      if (pattern == null) {
        pattern = convertToReqexp(patternStrings[idx]);
        patternMap.put(patternStrings[idx], pattern);
      }

      patterns[idx] = pattern;
    }

    return new HadoopSettlingMatcher(patternStrings, patterns);
  }

  private Pattern convertToReqexp(String regex)
  {
    if (regex == null) {
      return Pattern.compile(".*");
    }

    return Pattern.compile(
        regex.replace("\\N", ".*")
            .replaceAll("\\*", ".*")
            .replaceAll(",", "|")
    );
  }

  public void clear()
  {
    patternMap.clear();
  }
}
