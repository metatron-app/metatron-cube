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

package io.druid.java.util.common;

import java.util.Arrays;

public class VectorUtils
{
  public static float[] normalize(float[] vector)
  {
    final double norm = norm(vector);
    for (int i = 0; i < vector.length; i++) {
      vector[i] /= norm;
    }
    return vector;
  }

  private static double norm(float[] vector)
  {
    double s = 0;
    for (int i = 0; i < vector.length; i++) {
      s += vector[i] * vector[i];
    }
    return Math.sqrt(s);
  }

  public static byte[] clip(float[] vector)
  {
    byte[] b = new byte[vector.length];
    for (int i = 0; i < vector.length; i++) {
      b[i] = (byte) (vector[i] * 128);
    }
    return b;
  }

  public static byte[] quantize(float[] vector, float[][] minmax)
  {
    byte[] quantized = new byte[vector.length];
    for (int i = 0; i < vector.length; i++) {
      quantized[i] = (byte) x(vector[i], minmax[i]);
    }
    return quantized;
  }

  public static byte[] quantizeGlobal(float[] vector, float[] minmax)
  {
    byte[] quantized = new byte[vector.length];
    for (int i = 0; i < vector.length; i++) {
      quantized[i] = (byte) x(vector[i], minmax);
    }
    return quantized;
  }

  private static float[] globalMinmax(float[][] minmax)
  {
    float[] global = new float[] {minmax[0][0], minmax[0][1]};
    for (int i = 1; i < minmax.length; i++) {
      global[0] = Math.min(global[0], minmax[i][0]);
      global[1] = Math.max(global[1], minmax[i][1]);
    }
    return global;
  }

  private static int x(float c, float[] minmax)
  {
    float v = convert(c, minmax);
//    if (v > 0) {
//      v = v / minmax[1] * 127;
//    } else {
//      v = v / -minmax[0] * 128;
//    }
    int ix = (int) Math.floor(v);
    float frac = v - ix;
    return frac > 0.5 ? ix + 1 : ix;
  }

  private static float convert(float v, float[] minmax)
  {
    v -= minmax[0];
    v /= minmax[1] - minmax[0];
    return v * 255 - 128;
  }

  private static float revert(float v, float[] minmax)
  {
    v = (v + 128) / 255;
    v *= minmax[1] - minmax[0];
    v += minmax[0];
    return v;
  }

  private static float dot(float[] v1, float[] v2)
  {
    float s = 0;
    for (int i = 0; i < v1.length; i++) {
      s += v1[i] * v2[i];
    }
    return s;
  }

  private static float sim(byte[] b1, byte[] b2, float[][] minmax)
  {
    float s = 0;
    float bs1 = 0;
    float bs2 = 0;
    for (int i = 0; i < b1.length; i++) {
      float b1x = revert(b1[i], minmax[i]);
      float b2x = revert(b2[i], minmax[i]);
      s += b1x * b2x;
      bs1 += b1x * b1x;
      bs2 += b2x * b2x;
    }
    return (float) (s / Math.sqrt(bs1 * bs2));
  }

  private static float[][] minmax(int length, float[]... vectors)
  {
    float[][] minmax = new float[length][2];
    for (int i = 0; i < length; i++) {
      minmax[i][0] = minmax[i][1] = vectors[0][i];
    }
    for (int i = 0; i < length; i++) {
      for (int j = 1; j < vectors.length; j++) {
        minmax[i][0] = Math.min(minmax[i][0], vectors[j][i]);
        minmax[i][1] = Math.max(minmax[i][1], vectors[j][i]);
      }
    }
    return minmax;
  }

  public static void main(String[] args)
  {
    extracted(
        new float[]{0.5f, -0.5f, 0.5f, -0.5f},
        new float[]{0.9f, -0.1f, 0.2f, -0.4f},
        new float[]{-0.1f, -0.7f, 0.4f, 0.4f}
    );
  }

  private static void extracted(float[] v1, float[] v2, float[] v3)
  {
    v1 = normalize(v1);
    v2 = normalize(v2);
    v3 = normalize(v3);
    float[][] minmax = minmax(v1.length, v1, v2, v3);
    for (int i = 0; i < v1.length; i++) {
      System.out.printf("minmax%d = %s%n", i, Arrays.toString(minmax[i]));
    }
    float dot1 = dot(v1, v2);
    System.out.printf("%s dot %s = %s%n", Arrays.toString(v1), Arrays.toString(v2), dot1);
    float dot2 = dot(v1, v3);
    System.out.printf("%s dot %s = %s%n", Arrays.toString(v1), Arrays.toString(v3), dot2);
    float dot3 = dot(v2, v3);
    System.out.printf("%s dot %s = %s%n", Arrays.toString(v2), Arrays.toString(v3), dot3);
    byte[] b1 = quantize(v1, minmax);
    byte[] b2 = quantize(v2, minmax);
    byte[] b3 = quantize(v3, minmax);
    float sim1 = sim(b1, b2, minmax);
    System.out.printf("%s sim %s = %s%n", Arrays.toString(b1), Arrays.toString(b2), sim1);
    float sim2 = sim(b1, b3, minmax);
    System.out.printf("%s sim %s = %s%n", Arrays.toString(b1), Arrays.toString(b3), sim2);
    float sim3 = sim(b2, b3, minmax);
    System.out.printf("%s sim %s = %s%n", Arrays.toString(b2), Arrays.toString(b3), sim3);

    float[] vx = new float[]{0.023877744f, 0.04547623f, -0.0018790382f};
    for (float v :vx) {
      System.out.println(v + " = " + (int) (v * 255 - 128));
    }
  }
}
