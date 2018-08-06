/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.metamx.common.parsers.TimestampParser;
import io.druid.common.DateTimes;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Map;

/**
 */
@JsonTypeName("expression")
public class ExpressionTimestampSpec implements TimestampSpec
{
  private static final Function<String, DateTime> ISO_FORMAT = TimestampParser.createTimestampParser("iso");

  private final String expression;
  private final Expr parsed;

  @JsonCreator
  public ExpressionTimestampSpec(
      @JsonProperty("expression") String expression
  )
  {
    this.expression = Preconditions.checkNotNull(expression);
    this.parsed = Parser.parse(expression);
  }

  @Override
  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return expression;
  }

  @Override
  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = parsed.eval(Parser.withMap(input)).lhs;
    if (o == null) {
      return null;
    } else if (o instanceof DateTime) {
      return (DateTime) o;
    } else if (o instanceof Number) {
      return DateTimes.utc(((Number) o).longValue());
    } else if (o instanceof Timestamp) {
      return DateTimes.utc(((Timestamp) o).getTime());
    } else if (o instanceof String) {
      return ISO_FORMAT.apply((String) o);
    }
    return null;
  }
}
