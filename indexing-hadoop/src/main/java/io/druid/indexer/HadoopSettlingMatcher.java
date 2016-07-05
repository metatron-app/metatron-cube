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

import java.util.Arrays;
import java.util.regex.Pattern;

public class HadoopSettlingMatcher
{
  private final Object[] input;
  private final Pattern[] patterns;

  public HadoopSettlingMatcher(
      Object[] input,
      Pattern[] patterns
  )
  {
    this.input = input;
    this.patterns = patterns;
  }

  public boolean matches(String[] dimValues)
  {
    for (int idx = 0; idx < patterns.length; idx++) {
      Pattern pattern = patterns[idx];
      if (dimValues[idx] != null) {
        if (!pattern.matcher(dimValues[idx]).matches()) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    return Arrays.equals(input, ((HadoopSettlingMatcher) o).input);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(input);
  }

  @Override
  public String toString()
  {
    return Arrays.toString(input);
  }
}
