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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.data.Indexed;
import io.druid.segment.filter.DimensionPredicateFilter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class LikeDimFilter extends DimFilter.NotOptimizable
{
  // Regex matching characters that are definitely okay to include unescaped in a regex.
  // Leads to excessively paranoid escaping, although shouldn't affect runtime beyond compiling the regex.
  private static final Pattern DEFINITELY_FINE = Pattern.compile("[\\w\\d\\s-]");
  private static final String WILDCARD = ".*";

  private final String dimension;
  private final String pattern;
  private final Character escapeChar;
  private final ExtractionFn extractionFn;
  private final LikeMatcher likeMatcher;

  @JsonCreator
  public LikeDimFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("pattern") final String pattern,
      @JsonProperty("escape") final String escape,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern");
    this.extractionFn = extractionFn;

    if (escape != null && escape.length() != 1) {
      throw new IllegalArgumentException("Escape must be null or a single character");
    } else {
      this.escapeChar = (escape == null || escape.isEmpty()) ? null : escape.charAt(0);
    }

    this.likeMatcher = LikeMatcher.from(pattern, this.escapeChar);
  }

  public static class LikeMatcher implements Predicate<String>
  {
    public enum SuffixMatch
    {
      MATCH_ANY,
      MATCH_EMPTY,
      MATCH_PATTERN
    }

    // Strings match if:
    //  (a) suffixMatch is MATCH_ANY and they start with "prefix"
    //  (b) suffixMatch is MATCH_EMPTY and they start with "prefix" and contain nothing after prefix
    //  (c) suffixMatch is MATCH_PATTERN and the string matches "pattern"
    private final SuffixMatch suffixMatch;

    // Prefix that matching strings are known to start with. May be empty.
    private final String prefix;

    // Regex pattern that describes matching strings.
    private final Pattern pattern;

    private LikeMatcher(
        final SuffixMatch suffixMatch,
        final String prefix,
        final Pattern pattern
    )
    {
      this.suffixMatch = Preconditions.checkNotNull(suffixMatch, "suffixMatch");
      this.prefix = Strings.nullToEmpty(prefix);
      this.pattern = Preconditions.checkNotNull(pattern, "pattern");
    }

    public static LikeMatcher from(
        final String likePattern,
        @Nullable final Character escapeChar
    )
    {
      final StringBuilder prefix = new StringBuilder();
      final StringBuilder regex = new StringBuilder();
      boolean escaping = false;
      boolean inPrefix = true;
      SuffixMatch suffixMatch = SuffixMatch.MATCH_EMPTY;
      for (int i = 0; i < likePattern.length(); i++) {
        final char c = likePattern.charAt(i);
        if (escapeChar != null && c == escapeChar && !escaping) {
          escaping = true;
        } else if (c == '%' && !escaping) {
          inPrefix = false;
          if (suffixMatch == SuffixMatch.MATCH_EMPTY) {
            suffixMatch = SuffixMatch.MATCH_ANY;
          }
          regex.append(WILDCARD);
        } else if (c == '_' && !escaping) {
          inPrefix = false;
          suffixMatch = SuffixMatch.MATCH_PATTERN;
          regex.append(".");
        } else {
          if (inPrefix) {
            prefix.append(c);
          } else {
            suffixMatch = SuffixMatch.MATCH_PATTERN;
          }
          addPatternCharacter(regex, c);
          escaping = false;
        }
      }

      return new LikeMatcher(suffixMatch, prefix.toString(), Pattern.compile(regex.toString()));
    }

    private static void addPatternCharacter(final StringBuilder patternBuilder, final char c)
    {
      if (DEFINITELY_FINE.matcher(String.valueOf(c)).matches()) {
        patternBuilder.append(c);
      } else {
        patternBuilder.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }

    public boolean matches(@Nullable final String s)
    {
      String val = Strings.nullToEmpty(s);
      return val != null && pattern.matcher(val).matches();
    }

    /**
     * Checks if the suffix of strings.get(i) matches the suffix of this matcher. The first prefix.length characters
     * of s are ignored. This method is useful if you've already independently verified the prefix. This method
     * evalutes strings.get(i) lazily to save time when it isn't necessary to actually look at the string.
     */
    public boolean matchesSuffixOnly(final Indexed<String> strings, final int i)
    {
      if (suffixMatch == SuffixMatch.MATCH_ANY) {
        return true;
      } else if (suffixMatch == SuffixMatch.MATCH_EMPTY) {
        final String s = strings.get(i);
        return s == null ? matches(null) : s.length() == prefix.length();
      } else {
        // suffixMatch is MATCH_PATTERN
        final String s = strings.get(i);
        return matches(s);
      }
    }

    @Override
    public boolean apply(@Nullable String input)
    {
      return matches(input);
    }

    public String getPrefix()
    {
      return prefix;
    }

    public SuffixMatch getSuffixMatch()
    {
      return suffixMatch;
    }
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @JsonProperty
  public String getEscape()
  {
    return escapeChar != null ? escapeChar.toString() : null;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LIKE_CACHE_ID)
                  .append(dimension).sp()
                  .append(pattern).sp()
                  .append(escapeChar == null ? new byte[0] : Chars.toByteArray(escapeChar)).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(dimension);
    if (replaced == null || replaced.equals(dimension)) {
      return this;
    }
    return new LikeDimFilter(replaced, getPattern(), getEscape(), getExtractionFn());
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new DimensionPredicateFilter(dimension, likeMatcher, extractionFn);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LikeDimFilter that = (LikeDimFilter) o;

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) {
      return false;
    }
    if (escapeChar != null ? !escapeChar.equals(that.escapeChar) : that.escapeChar != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
    result = 31 * result + (escapeChar != null ? escapeChar.hashCode() : 0);
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder builder = new StringBuilder();

    if (extractionFn != null) {
      builder.append(extractionFn).append("(");
    }

    builder.append(dimension);

    if (extractionFn != null) {
      builder.append(")");
    }

    builder.append(" LIKE '").append(pattern).append("'");

    if (escapeChar != null) {
      builder.append(" ESCAPE '").append(escapeChar).append("'");
    }

    return builder.toString();
  }
}
