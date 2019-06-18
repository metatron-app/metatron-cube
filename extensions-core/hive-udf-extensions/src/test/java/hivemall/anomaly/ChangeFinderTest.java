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
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

// ported from hivemall
public class ChangeFinderTest
{
  static {
    Parser.register(HivemallFunctions.class);
  }

  private static final boolean DEBUG = false;

  @Test
  public void testCf1d() throws IOException, HiveException
  {
    DSuppliers.HandOver<Double> provider = new DSuppliers.HandOver<>();
    Expr expr = Parser.parse("changefinder(x, lossFunc='logloss')");
    Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of("x", provider));

    BufferedReader reader = readFile("cf1d.csv.gz");
    println("x outlier change");
    String line;
    int numOutliers = 0, numChangepoints = 0;
    while ((line = reader.readLine()) != null) {
      double x = Double.parseDouble(line);
      provider.set(x);
      ExprEval eval = Evals.eval(expr, binding);
      List value = (List) eval.value();
      printf("%f %f %f%n", x, value.get(0), value.get(1));
      if ((Double) value.get(0) > 10.d) {
        numOutliers++;
      }
      if ((Double) value.get(1) > 10.d) {
        numChangepoints++;
      }
    }
    Assert.assertTrue("#outliers SHOULD be greater than 10: " + numOutliers, numOutliers > 10);
    Assert.assertTrue("#outliers SHOULD be less than 20: " + numOutliers, numOutliers < 20);
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
  public void testTwitterData() throws IOException, HiveException
  {
    DSuppliers.HandOver<Double> provider = new DSuppliers.HandOver<>();
    Expr expr = Parser.parse("changefinder(x, lossFunc='logloss',r1=0.01,k=6,T1=10,T2=5)");
    Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of("x", provider));

    BufferedReader reader = readFile("twitter.csv.gz");
    println("# time x outlier change");
    String line;
    int i = 1, numOutliers = 0, numChangepoints = 0;
    while ((line = reader.readLine()) != null) {
      double x = Double.parseDouble(line);
      provider.set(x);
      ExprEval eval = Evals.eval(expr, binding);
      List value = (List) eval.value();
      printf("%f %f %f%n", x, value.get(0), value.get(1));
      if ((Double) value.get(0) > 30.d) {
        numOutliers++;
      }
      if ((Double) value.get(1) > 8.d) {
        numChangepoints++;
      }
      i++;
    }
    Assert.assertTrue("#outliers SHOULD be greater than 5: " + numOutliers, numOutliers > 5);
    Assert.assertTrue("#outliers SHOULD be less than 10: " + numOutliers, numOutliers < 10);
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
  public void testPoissenDist() throws HiveException
  {
    DSuppliers.HandOver<Double> provider1 = new DSuppliers.HandOver<>();
    DSuppliers.HandOver<Double> provider2 = new DSuppliers.HandOver<>();
    DSuppliers.HandOver<Double> provider3 = new DSuppliers.HandOver<>();

    Expr expr = Parser.parse("changefinder(x, y, z, lossFunc='logloss',r1=0.01,k=6,T1=10,T2=5)");
    Expr.NumericBinding binding = Parser.withSuppliers(
        ImmutableMap.<String, Supplier>of("x", provider1, "y", provider2, "z", provider3)
    );

    final int examples = 10000;
    final int dims = 3;
    final PoissonDistribution[] poisson = new PoissonDistribution[]{
        new PoissonDistribution(10.d), new PoissonDistribution(5.d),
        new PoissonDistribution(20.d)
    };
    final Random rand = new Random(42);
    println("# time x0 x1 x2 outlier change");
    for (int i = 0; i < examples; i++) {
      double r = rand.nextDouble();
      double x = r * poisson[0].sample();
      double y = r * poisson[1].sample();
      double z = r * poisson[2].sample();
      provider1.set(x);
      provider2.set(y);
      provider3.set(z);

      ExprEval eval = Evals.eval(expr, binding);
      List value = (List) eval.value();
      printf("%d %f %f %f %f %f%n", i, x, y, z, value.get(0), value.get(1));
    }
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
    InputStream is = ChangeFinderTest.class.getResourceAsStream(fileName);
    if (fileName.endsWith(".gz")) {
      is = new GZIPInputStream(is);
    }
    return new BufferedReader(new InputStreamReader(is));
  }
}
