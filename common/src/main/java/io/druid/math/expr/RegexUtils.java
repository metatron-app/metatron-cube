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

package io.druid.math.expr;

import com.metamx.common.Pair;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copied from org.apache.hadoop.hive.ql.udf.UDFLike
 */
public class RegexUtils
{
  public static enum PatternType
  {
    NONE, // "abc"
    BEGIN, // "abc%"
    END, // "%abc"
    MIDDLE, // "%abc%"
    COMPLEX, // all other cases, such as "ab%c_de"
  }

  public static Pair<PatternType, Object> parse(String likePattern)
  {
    int length = likePattern.length();
    int beginIndex = 0;
    int endIndex = length;
    char lastChar = 'a';
    String strPattern = "";
    PatternType type = PatternType.NONE;

    for (int i = 0; i < length; i++) {
      char n = likePattern.charAt(i);
      if (n == '_') { // such as "a_b"
        if (lastChar != '\\') { // such as "a%bc"
          type = PatternType.COMPLEX;
          break;
        } else { // such as "abc\%de%"
          strPattern += likePattern.substring(beginIndex, i - 1);
          beginIndex = i;
        }
      } else if (n == '%') {
        if (i == 0) { // such as "%abc"
          type = PatternType.END;
          beginIndex = 1;
        } else if (i < length - 1) {
          if (lastChar != '\\') { // such as "a%bc"
            type = PatternType.COMPLEX;
            break;
          } else { // such as "abc\%de%"
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
          }
        } else {
          if (lastChar != '\\') {
            endIndex = length - 1;
            if (type == PatternType.END) { // such as "%abc%"
              type = PatternType.MIDDLE;
            } else {
              type = PatternType.BEGIN; // such as "abc%"
            }
          } else { // such as "abc\%"
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
            endIndex = length;
          }
        }
      }
      lastChar = n;
    }

    if (type == PatternType.COMPLEX) {
      return Pair.<PatternType, Object>of(type, Pattern.compile(likePatternToRegExp(likePattern)).matcher(""));
    }
    strPattern += likePattern.substring(beginIndex, endIndex);
    return Pair.<PatternType, Object>of(type, strPattern);
  }

  private static String likePatternToRegExp(String likePattern)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < likePattern.length(); i++) {
      // Make a special case for "\\_" and "\\%"
      char n = likePattern.charAt(i);
      if (n == '\\'
          && i + 1 < likePattern.length()
          && (likePattern.charAt(i + 1) == '_' || likePattern.charAt(i + 1) == '%')) {
        sb.append(likePattern.charAt(i + 1));
        i++;
        continue;
      }

      if (n == '_') {
        sb.append(".");
      } else if (n == '%') {
        sb.append(".*");
      } else {
        sb.append(Pattern.quote(Character.toString(n)));
      }
    }
    return sb.toString();
  }

  private static boolean find(String s, String sub, int startS, int endS)
  {
    char[] charS = s.toCharArray();
    char[] charSub = sub.toCharArray();
    int lenSub = sub.length();
    boolean match = false;
    for (int i = startS; (i < endS - lenSub + 1) && (!match); i++) {
      match = true;
      for (int j = 0; j < lenSub; j++) {
        if (charS[j + i] != charSub[j]) {
          match = false;
          break;
        }
      }
    }
    return match;
  }

  public static boolean evaluate(String s, PatternType type, Object matcher)
  {
    if (s == null) {
      return false;
    }

    if (type == PatternType.COMPLEX) {
      Matcher m = ((Matcher)matcher).reset(s);
      return m.matches();
    }

    String simplePattern = (String)matcher;
    int startS = 0;
    int endS = s.length();
    // if s is shorter than the required pattern
    if (endS < simplePattern.length()) {
      return false;
    }
    switch (type) {
      case BEGIN:
        endS = simplePattern.length();
        break;
      case END:
        startS = endS - simplePattern.length();
        break;
      case NONE:
        if (simplePattern.length() != s.length()) {
          return false;
        }
        break;
      case MIDDLE:
        break;
      default:
        throw new IllegalStateException("should not be happened");
    }
    return find(s, simplePattern, startS, endS);
  }
}
