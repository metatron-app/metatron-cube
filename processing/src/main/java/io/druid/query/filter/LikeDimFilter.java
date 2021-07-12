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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.filter.DimensionPredicateFilter;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.PrefixFilter;
import io.druid.segment.filter.SelectorFilter;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LikeDimFilter extends SingleInput
{
  // Regex matching characters that are definitely okay to include unescaped in a regex.
  // Leads to excessively paranoid escaping, although shouldn't affect runtime beyond compiling the regex.
  private static final Matcher DEFINITELY_FINE = Pattern.compile("[\\w\\d\\s-]").matcher("");
  private static final String WILDCARD = ".*";

  private final String dimension;
  private final String pattern;
  private final Character escapeChar;
  private final ExtractionFn extractionFn;
  private final Supplier<LikeMatcher> likeMatcherSupplier;

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
    }
    this.escapeChar = StringUtils.isNullOrEmpty(escape) ? null : escape.charAt(0);
    this.likeMatcherSupplier = Suppliers.memoize(() -> LikeMatcher.from(pattern, escapeChar));
  }

  private static final CharSet REGEX_CHARS = new CharOpenHashSet(
      new char[]{'?', '[', '\\', ']', '^'}
  );

  private static boolean isNonRegexAsciiChar(char x)
  {
    return x == ' ' || (x >= '0' && x <= 'z' && !REGEX_CHARS.contains(x));
  }

  private static boolean isLikePattern(char x)
  {
    return x == '_' || x == '%';
  }

  public static class LikeMatcher
  {
    private final String pattern;
    // Prefix that matching strings are known to start with. May be empty.
    private final String prefix;
    private final String suffix;
    private final List<String> elements;

    // Regex pattern that describes matching strings.
    private final String regex;

    public LikeMatcher(
        final String pattern,
        final String prefix,
        final String suffix,
        final List<String> elements,
        final String regex
    )
    {
      this.pattern = pattern;
      this.prefix = prefix;
      this.suffix = suffix;
      this.elements = elements;
      this.regex = regex;
    }

    public static LikeMatcher from(
        final String pattern,
        @Nullable final Character escapeChar
    )
    {
      final StringBuilder builder = new StringBuilder();
      boolean escaping = false;
      for (int i = 0; i < pattern.length(); i++) {
        final char c = pattern.charAt(i);
        if (escapeChar != null && c == escapeChar && !escaping) {
          escaping = true;
        } else if (c == '%' && !escaping) {
          builder.append(WILDCARD);
        } else if (c == '_' && !escaping) {
          builder.append(".");
        } else {
          if (DEFINITELY_FINE.reset(String.valueOf(c)).matches()) {
            builder.append(c);
          } else {
            builder.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
          }
          escaping = false;
        }
      }

      int prev = 0;
      List<int[]> strings = Lists.newArrayList();
      int p = 0;
      for (; p < pattern.length(); p++) {
        if (!isNonRegexAsciiChar(pattern.charAt(p))) {
          if (p > prev) {
            strings.add(new int[]{prev, p});
          }
          if (!isLikePattern(pattern.charAt(p))) {
            break;
          }
          prev = p + 1;
        }
      }
      if (prev == 0 && p == pattern.length()) {
        return new LikeMatcher(pattern, pattern, null, Arrays.asList(), null);
      }
      int s = -1;
      for (int i = pattern.length() - 1; i >= 0; i--) {
        if (!isNonRegexAsciiChar(pattern.charAt(i))) {
          if (isLikePattern(pattern.charAt(i))) {
            s = i;
          }
          break;
        }
      }
      String prefix = null;
      if (!strings.isEmpty() && strings.get(0)[0] == 0) {
        prefix = pattern.substring(0, strings.get(0)[1]);
        strings = strings.subList(1, strings.size());
      }
      List<String> elements = Lists.newArrayList(Iterables.transform(strings, x -> pattern.substring(x[0], x[1])));
      String suffix = s >= 0 && s < pattern.length() - 1 ? pattern.substring(s + 1, pattern.length()) : null;
      return new LikeMatcher(pattern, prefix, suffix, elements, builder.toString());
    }

    public int predicateType()
    {
      if (pattern.equals(prefix)) {
        return 0;
      }
      final int patternLen = pattern.length();

      int type = 0;
      if (prefix != null) {
        type += 1;
        if (patternLen == prefix.length() + 1 && isLikePattern(pattern.charAt(patternLen - 1))) {
          return type;
        }
      }
      if (suffix != null) {
        type += 2;
        if (patternLen == suffix.length() + 1 && isLikePattern(pattern.charAt(0))) {
          return type;
        }
      }
      if (prefix != null && suffix != null) {
        if (patternLen == prefix.length() + suffix.length() + 1 && isLikePattern(pattern.charAt(prefix.length()))) {
          return type;
        }
      }
      if (!elements.isEmpty()) {
        type += 4;
      }
      if (regex != null) {
        type += 8;
      }
      return type;
    }

    public Predicate<String> asPredicate()
    {
      if (pattern.equals(prefix)) {
        return s -> s.equals(prefix);
      }
      List<Predicate<String>> predicates = Lists.newArrayList();
      if (prefix != null) {
        if (pattern.length() == prefix.length() + 1 && isLikePattern(pattern.charAt(pattern.length() - 1))) {
          return s -> s.startsWith(prefix);
        }
        predicates.add(s -> s.startsWith(prefix));
      }
      if (suffix != null) {
        if (pattern.length() == suffix.length() + 1 && isLikePattern(pattern.charAt(0))) {
          return s -> s.endsWith(suffix);
        }
        predicates.add(s -> s.endsWith(suffix));
      }
      if (prefix != null && suffix != null) {
        if (pattern.length() == prefix.length() + suffix.length() + 1 && isLikePattern(pattern.charAt(prefix.length()))) {
          return Predicates.and(predicates);
        }
      }
      for (String element : elements) {
        predicates.add(s -> s.contains(element));
      }
      if (regex != null) {
        Matcher matcher = Pattern.compile(regex, Pattern.DOTALL).matcher("");
        predicates.add(s -> matcher.reset(s).matches());
      }
      return Predicates.and(predicates);
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
      LikeMatcher matcher1 = (LikeMatcher) o;
      return Objects.equals(prefix, matcher1.prefix) &&
             Objects.equals(suffix, matcher1.suffix) &&
             Objects.equals(elements, matcher1.elements) &&
             Objects.equals(regex, matcher1.regex);
    }

    @Override
    public String toString()
    {
      return "LikeMatcher{" +
             "prefix='" + prefix + '\'' +
             ", suffix='" + suffix + '\'' +
             ", elements=" + elements +
             ", regex='" + regex + '\'' +
             '}';
    }
  }

  @Override
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getEscape()
  {
    return escapeChar != null ? escapeChar.toString() : null;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.LIKE_CACHE_ID)
                  .append(dimension).sp()
                  .append(pattern).sp()
                  .append(escapeChar == null ? new byte[0] : Chars.toByteArray(escapeChar)).sp()
                  .append(extractionFn);
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new LikeDimFilter(dimension, getPattern(), getEscape(), getExtractionFn());
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    final LikeMatcher matcher = likeMatcherSupplier.get();
    if (extractionFn == null) {
      switch (matcher.predicateType()) {
        case 0:
          return new SelectorFilter(dimension, matcher.prefix);
        case 1:
          return new PrefixFilter(dimension, matcher.prefix);
      }
    }
    if (Strings.isNullOrEmpty(matcher.prefix)) {
      return new DimensionPredicateFilter(dimension, matcher.asPredicate(), extractionFn);
    }
    return new DimensionPredicateFilter(dimension, matcher.asPredicate(), extractionFn)
    {
      @Override
      protected Filters.DictionaryMatcher<String> toMatcher(Predicate<String> predicate)
      {
        return new Filters.FromPredicate<String>(matcher.prefix, matcher.asPredicate());
      }
    };
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
