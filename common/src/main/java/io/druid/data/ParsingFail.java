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

package io.druid.data;

import com.google.common.base.Strings;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.parsers.ParseException;

import java.util.Objects;

/**
 */
public class ParsingFail extends ParseException
{
  private final Object input;

  public ParsingFail(Object input, Throwable cause, String message, Object... params)
  {
    super(cause, message, params);
    this.input = input;
  }

  public ParsingFail(Object input, Throwable cause)
  {
    super(cause, Strings.nullToEmpty(cause.getMessage()));
    this.input = input;
  }

  public ParsingFail(Object input, String message, Object... params)
  {
    super(message, params);
    this.input = input;
  }

  public Object getInput()
  {
    return input;
  }

  @Override
  public String getMessage()
  {
    String string = Objects.toString(input, null);
    return super.getMessage() + " from " + StringUtils.limit(string, 128);
  }

  public static ParseException propagate(Object input, Throwable t)
  {
    throw propagate(input, t, Strings.nullToEmpty(t.getMessage()));
  }

  public static ParseException propagate(Object input, Throwable t, String message, Object... params)
  {
    if (t instanceof ParseException) {
      throw (ParseException) t;
    }
    throw new ParsingFail(input, message, t, params);
  }
}
