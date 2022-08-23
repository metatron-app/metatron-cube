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

package io.druid.math.expr;

public enum Arithmetics
{
  PLUS {
    @Override
    public float op(float v1, float v2) {return v1 + v2;}

    @Override
    public double op(double v1, double v2) {return v1 + v2;}

    @Override
    public long op(long v1, long v2) {return v1 + v2;}
  },
  MINUS {
    @Override
    public float op(float v1, float v2) {return v1 - v2;}

    @Override
    public double op(double v1, double v2) {return v1 - v2;}

    @Override
    public long op(long v1, long v2) {return v1 - v2;}
  },
  MULTIPLY {
    @Override
    public float op(float v1, float v2) {return v1 * v2;}

    @Override
    public double op(double v1, double v2) {return v1 * v2;}

    @Override
    public long op(long v1, long v2) {return v1 * v2;}
  },
  DIV {
    @Override
    public float op(float v1, float v2) {return v1 / v2;}

    @Override
    public double op(double v1, double v2) {return v1 / v2;}

    @Override
    public long op(long v1, long v2) {return v1 / v2;}
  };

  public abstract float op(float v1, float v2);

  public abstract double op(double v1, double v2);

  public abstract long op(long v1, long v2);

  public static Arithmetics of(String v)
  {
    switch (v) {
      case "+": return PLUS;
      case "-": return MINUS;
      case "*": return MULTIPLY;
      case "/": return DIV;
    }
    return null;
  }
}
