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

package io.druid.query.aggregation.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HoltWintersModelTest
{
  @Test
  public void testPrediction()
  {
    HoltWintersModel model = new HoltWintersModel();

    List<Integer> v1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

    Assert.assertArrayEquals(
        new double[]{9.58918223095513, 10.41367737830126, 11.23817252564739, 12.06266767299352},
        model.doPredict(v1, 4),
        0.0001
    );

    List<Integer> v2 = Arrays.asList(1, 3, 5, 6, 7, 8, 7, 6);

    Assert.assertArrayEquals(
        new double[]{10.43831015704053, 11.78145421425206, 13.12459827146359, 14.467742328675119},
        model.doPredict(v2, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{10.893285852386418, 11.726038582641184, 13.505413739926931, 14.338166470181697},
        model.withPeriod(2).doPredict(v2, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{9.211985352672619, 10.130548281, 10.393297298613094, 10.748485716285714},
        model.withPeriod(4).doPredict(v2, 4),
        0.0001
    );

    List<Integer> v3 = Arrays.asList(1, 2, 4, 2, 3, 6, 3, 5);

    Assert.assertArrayEquals(
        new double[]{6.44502125244613, 7.13905392498326, 7.833086597520391, 8.527119270057522},
        model.doPredict(v3, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{5.496114802967353, 6.955873094239708, 6.651111584029978, 8.110869875302333},
        model.withPeriod(2).doPredict(v3, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{6.636190979752596, 5.000623888371716, 6.328489094291702, 7.832327849116449},
        model.withPeriod(3).doPredict(v3, 4),
        0.0001
    );

    // make more sensitive to change
    model = model.withAlpha(0.5f).withBeta(0.3f);

    Assert.assertArrayEquals(
        new double[]{5.204000093311096, 5.472385440068775, 5.740770786826454, 6.009156133584133},
        model.doPredict(v3, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{4.462219121567724, 5.67895643377857, 4.957955663575876, 6.174692975786722},
        model.withPeriod(2).doPredict(v3, 4),
        0.0001
    );

    Assert.assertArrayEquals(
        new double[]{6.555621399002925, 4.865570399227553, 6.211528741648581, 7.689578857278863},
        model.withPeriod(3).doPredict(v3, 4),
        0.0001
    );
  }
}
