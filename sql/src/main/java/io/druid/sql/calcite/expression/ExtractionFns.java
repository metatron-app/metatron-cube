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

package io.druid.sql.calcite.expression;

import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;

import java.util.ArrayList;
import java.util.List;

public class ExtractionFns
{
  /**
   * Cascade f and g, returning an ExtractionFn that computes g(f(x)). Null f or g are treated like identity functions.
   *
   * @param f function
   * @param g function
   *
   * @return composed function, or null if both f and g were null
   */
  public static ExtractionFn cascade(final ExtractionFn f, final ExtractionFn g)
  {
    if (f == null) {
      // Treat null like identity.
      return g;
    } else if (g == null) {
      return f;
    } else {
      final List<ExtractionFn> extractionFns = new ArrayList<>();

      // Apply g, then f, unwrapping if they are already cascades.

      if (f instanceof CascadeExtractionFn) {
        extractionFns.addAll(((CascadeExtractionFn) f).getExtractionFns());
      } else {
        extractionFns.add(f);
      }

      if (g instanceof CascadeExtractionFn) {
        extractionFns.addAll(((CascadeExtractionFn) g).getExtractionFns());
      } else {
        extractionFns.add(g);
      }

      return new CascadeExtractionFn(extractionFns);
    }
  }
}
