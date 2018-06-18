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

package io.druid.data;

import com.metamx.common.parsers.ParseException;

/**
 */
public class ParsingFail extends ParseException
{
  private final Object input;

  public ParsingFail(Object input, String message, Throwable cause)
  {
    super(message, cause);
    this.input = input;
  }

  public ParsingFail(Object input, Throwable cause)
  {
    super(cause, cause.getMessage());
    this.input = input;
  }

  public Object getInput()
  {
    return input;
  }

  public static ParseException propagate(Object input, Throwable t)
  {
    if (t instanceof ParseException) {
      throw (ParseException) t;
    }
    throw new ParsingFail(input, t);
  }
}
