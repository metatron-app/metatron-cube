/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query.dimension;

import java.util.Arrays;

public interface DictionaryID
{
  static int[] bitsRequired(int[] cardinalities)
  {
    final int[] bits = new int[cardinalities.length];
    for (int i = 0; i < cardinalities.length; i++) {
      if (cardinalities[i] < 0) {
        return null;
      }
      bits[i] = bitsRequired(cardinalities[i]);
    }
    return bits;
  }

  static int[] bitsToShifts(int[] bits)
  {
    final int[] shifts = new int[bits.length];
    for (int i = bits.length - 2; i >= 0; i--) {
      shifts[i] = shifts[i + 1] + bits[i + 1];
    }
    return shifts;
  }

  static int[] bitsToMasks(int[] bits)
  {
    return Arrays.stream(bits).map(DictionaryID::bitToMask).toArray();
  }

  static int bitToMask(int bit)
  {
    return (1 << bit) - 1;
  }

  static int bitsRequired(int cardinality)
  {
    if (cardinality == 1) {
      return 1;
    }
    final double v = Math.log(cardinality) / Math.log(2);
    return v == (int) v ? (int) v : (int) v + 1;
  }
}
