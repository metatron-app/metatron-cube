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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class LikeDimFilter extends SingleInput
{
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

  public static class LikeMatcher
  {
    // Prefix that matching strings are known to start with. May be empty.
    private final String prefix;
    private final Node[] elements;

    private LikeMatcher(String prefix, Node[] elements)
    {
      this.prefix = prefix;
      this.elements = elements;
    }

    public static LikeMatcher from(final String pattern, @Nullable final Character escapeChar)
    {
      int px = 0;
      List<Node> elements = Lists.newArrayList();
      for (int i = 0; i < pattern.length(); i++) {
        final char c = pattern.charAt(i);
        if (escapeChar != null && c == escapeChar) {
          i++;
        } else if (c == '%' || c == '_') {
          if (i > px) {
            elements.add(new Literal(pattern.substring(px, i)));
          }
          elements.add(c == '_' ? new Underbar() : new Percent());
          px = i + 1;
        }
      }
      if (px < pattern.length()) {
        elements.add(new Literal(pattern.substring(px)));
      }
      Node first = elements.get(0);
      String prefix = first instanceof Literal ? ((Literal) first).v : null;
      return new LikeMatcher(prefix, compress(elements));
    }

    private static Node[] compress(List<Node> raw)
    {
      List<Node> compressed = Lists.newArrayList();
      for (int i = 0; i < raw.size(); i++) {
        Node value = raw.get(i);
        if (value instanceof Literal) {
          compressed.add(value);
          continue;
        }
        int x = findNext(raw, i + 1, n -> n instanceof Literal);
        if (x == i + 1) {
          compressed.add(value);
          continue;
        }
        int underbars = (int) raw.subList(i, x).stream().filter(s -> s instanceof Underbar).count();
        if (underbars == 0) {
          compressed.add(value);
        } else if (underbars == x - i) {
          compressed.add(new Underbar(underbars));
        } else {
          compressed.add(new Underbar(underbars));
          compressed.add(new Percent());
        }
        i = x - 1;
      }
      return compressed.toArray(new Node[0]);
    }

    private static int findNext(List<Node> values, int start, Predicate<Node> predicate)
    {
      int i = start;
      for (; i < values.size(); i++) {
        if (predicate.apply(values.get(i))) {
          break;
        }
      }
      return i;
    }

    public Predicate<String> asPredicate()
    {
      final Predicate<String> predicate = _asPredicate();
      return s -> s != null && predicate.apply(s);
    }

    private Predicate<String> _asPredicate()
    {
      switch (represent()) {
        case "%":
          return Predicates.alwaysTrue();
        case "_": {
          final int c = ((Underbar) elements[0]).c;
          return s -> s.length() == c;
        }
        case "L": {
          final String v = ((Literal) elements[0]).v;
          return s -> s.equals(v);
        }
        case "L%": {
          final String v = ((Literal) elements[0]).v;
          final int length = v.length();
          return s -> s.length() >= length && s.startsWith(v);
        }
        case "L_": {
          final String v = ((Literal) elements[0]).v;
          final int length = v.length() + ((Underbar) elements[1]).c;
          return s -> s.length() == length && s.startsWith(v);
        }
        case "%L": {
          final String v = ((Literal) elements[1]).v;
          final int length = v.length();
          return s -> s.length() >= length && s.endsWith(v);
        }
        case "_L": {
          final String v = ((Literal) elements[1]).v;
          final int length = v.length() + ((Underbar) elements[0]).c;
          return s -> s.length() == length && s.endsWith(v);
        }
        case "L%L": {
          final String v1 = ((Literal) elements[0]).v;
          final String v2 = ((Literal) elements[2]).v;
          final int length = v1.length() + v2.length();
          return s -> s.length() >= length && s.startsWith(v1) && s.endsWith(v2);
        }
        case "L_L": {
          final String v1 = ((Literal) elements[0]).v;
          final String v2 = ((Literal) elements[2]).v;
          final int length = v1.length() + v2.length() + ((Underbar) elements[1]).c;
          return s -> s.length() == length && s.startsWith(v1) && s.endsWith(v2);
        }
        case "%L%": {
          final String v = ((Literal) elements[1]).v;
          return s -> s.contains(v);
        }
        case "_L%": {
          final String v = ((Literal) elements[1]).v;
          final int c = ((Underbar) elements[0]).c;
          final int length = v.length() + c;
          return s -> s.length() >= length && s.startsWith(v, c);
        }
        case "%L_": {
          final String v = ((Literal) elements[1]).v;
          final int c = ((Underbar) elements[2]).c;
          final int length = v.length() + c;
          return s -> s.length() >= length && s.startsWith(v, s.length() - length);
        }
        case "_L_": {
          final int c1 = ((Underbar) elements[0]).c;
          final int c2 = ((Underbar) elements[2]).c;
          final String v = ((Literal) elements[1]).v;
          final int length = v.length() + c1 + c2;
          return s -> s.length() == length && s.startsWith(v, c1);
        }
      }
//      Matcher matcher = Pattern.compile(toRegex(), Pattern.DOTALL).matcher("");
//      return s -> matcher.reset(s).matches();
      final State ix = new State();
      return s -> {
        ix.reset();
        for (int i = 0; i < elements.length; i++) {
          if (!elements[i].process(s, ix)) {
            return false;
          }
        }
        return ix.seek || ix.index == s.length();
      };
    }

    // ternary?
    public String represent()
    {
      char[] represent = new char[elements.length];
      for (int i = 0; i < elements.length; i++) {
        represent[i] = elements[i].represent();
      }
      return new String(represent);
    }

    private static class State
    {
      int index;
      boolean seek;

      private void reset()
      {
        index = 0;
        seek = false;
      }
    }

    public String regex()
    {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < elements.length; i++) {
        elements[i].regex(b);
      }
      return b.toString();
    }

    private static interface Node
    {
      void regex(StringBuilder builder);

      boolean process(String value, State ix);

      char represent();
    }

    private static class Literal implements Node
    {
      private final String v;

      private Literal(String v) {this.v = v;}

      @Override
      public void regex(StringBuilder builder)
      {
        builder.append("\\Q").append(v).append("\\E");
      }

      @Override
      public boolean process(String value, State ix)
      {
        if (ix.seek) {
          final int x = value.indexOf(v, ix.index);
          if (x < 0) {
            return false;
          }
          ix.seek = false;
          ix.index = x + v.length();
          return true;
        } else if (value.startsWith(v, ix.index)) {
          ix.index += v.length();
          return true;
        }
        return false;
      }

      @Override
      public char represent()
      {
        return 'L';
      }
    }

    private static class Underbar implements Node
    {
      private final int c;

      private Underbar(int c) {this.c = c;}

      private Underbar() {this(1);}

      @Override
      public void regex(StringBuilder builder)
      {
        builder.append('.');
        if (c > 1) {
          builder.append('{').append(c).append('}');
        }
      }

      @Override
      public boolean process(String value, State ix)
      {
        return (ix.index += c) < value.length();
      }

      @Override
      public char represent()
      {
        return '_';
      }
    }

    private static class Percent implements Node
    {
      @Override
      public void regex(StringBuilder builder)
      {
        builder.append(".*");
      }

      @Override
      public boolean process(String value, State ix)
      {
        ix.seek = true;
        return true;
      }

      @Override
      public char represent()
      {
        return '%';
      }
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
      switch (matcher.represent()) {
        case "L":
          return new SelectorFilter(dimension, matcher.prefix);
        case "L%":
          return new PrefixFilter(dimension, matcher.prefix);
      }
    }
    if (matcher.prefix == null) {
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

    if (!Objects.equals(dimension, that.dimension)) {
      return false;
    }
    if (!Objects.equals(pattern, that.pattern)) {
      return false;
    }
    if (!Objects.equals(escapeChar, that.escapeChar)) {
      return false;
    }
    return Objects.equals(extractionFn, that.extractionFn);

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
