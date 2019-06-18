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
package hivemall.anomaly;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.DSuppliers;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class SingularSpectrumTransformTest
{
  static {
    Parser.register(HivemallFunctions.class);
  }

  private static final boolean DEBUG = false;

  @Test
  public void testSVDSST() throws IOException, HiveException
  {
    int numChangepoints = detectSST("sst(x, scorefunc='svd')", 0.95d);
    Assert.assertTrue(
        "#changepoints SHOULD be greater than 0: " + numChangepoints,
        numChangepoints > 0
    );
    Assert.assertTrue(
        "#changepoints SHOULD be less than 5: " + numChangepoints,
        numChangepoints < 5
    );
  }

  @Test
  public void testIKASST() throws IOException, HiveException
  {
    int numChangepoints = detectSST("sst(x, scorefunc='ika')", 0.65d);
    Assert.assertTrue(
        "#changepoints SHOULD be greater than 0: " + numChangepoints,
        numChangepoints > 0
    );
    Assert.assertTrue(
        "#changepoints SHOULD be less than 5: " + numChangepoints,
        numChangepoints < 5
    );
  }

  @Test
  public void testSVDTwitterData() throws IOException, HiveException
  {
    int numChangepoints = detectTwitterData("sst(x, scorefunc='svd')", 0.005d);
    Assert.assertTrue(
        "#changepoints SHOULD be greater than 0: " + numChangepoints,
        numChangepoints > 0
    );
    Assert.assertTrue(
        "#changepoints SHOULD be less than 5: " + numChangepoints,
        numChangepoints < 5
    );
  }

  @Test
  public void testIKATwitterData() throws IOException, HiveException
  {
    int numChangepoints = detectTwitterData("sst(x, scorefunc='ika')", 0.0175d);
    Assert.assertTrue(
        "#changepoints SHOULD be greater than 0: " + numChangepoints,
        numChangepoints > 0
    );
    Assert.assertTrue(
        "#changepoints SHOULD be less than 5: " + numChangepoints,
        numChangepoints < 5
    );
  }

  private static int detectSST(final String expression, final double threshold) throws IOException, HiveException
  {
    Expr expr = Parser.parse(expression);
    DSuppliers.HandOver<Double> provider = new DSuppliers.HandOver<>();
    Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of("x", provider));

    BufferedReader reader = readFile("cf1d.csv.gz");
    println("x change");
    String line;
    int numChangepoints = 0;
    while ((line = reader.readLine()) != null) {
      double x = Double.parseDouble(line);
      provider.set(x);
      ExprEval eval = Evals.eval(expr, binding);
      printf("%f %f%n", x, eval.asDouble());
      if (eval.asDouble() > threshold) {
        numChangepoints++;
      }
    }

    return numChangepoints;
  }

  private static int detectTwitterData(final String expression, final double threshold)
      throws IOException, HiveException
  {
    Expr expr = Parser.parse(expression);
    DSuppliers.HandOver<Double> provider = new DSuppliers.HandOver<>();
    Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of("x", provider));

    BufferedReader reader = readFile("twitter.csv.gz");
    println("# time x change");
    String line;
    int i = 1, numChangepoints = 0;
    while ((line = reader.readLine()) != null) {
      double x = Double.parseDouble(line);
      provider.set(x);
      ExprEval eval = Evals.eval(expr, binding);
      printf("%d %f %f%n", i, x, eval.asDouble());
      if (eval.asDouble() > threshold) {
        numChangepoints++;
      }
      i++;
    }

    return numChangepoints;
  }

  private static void println(String msg)
  {
    if (DEBUG) {
      System.out.println(msg);
    }
  }

  private static void printf(String format, Object... args)
  {
    if (DEBUG) {
      System.out.printf(format, args);
    }
  }

  @Nonnull
  private static BufferedReader readFile(@Nonnull String fileName) throws IOException
  {
    InputStream is = SingularSpectrumTransformTest.class.getResourceAsStream(fileName);
    if (fileName.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    return new BufferedReader(new InputStreamReader(is));
  }
}
