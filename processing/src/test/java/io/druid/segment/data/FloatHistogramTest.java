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

package io.druid.segment.data;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import io.druid.segment.data.MetricBitmaps.FloatBitmaps;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

public class FloatHistogramTest
{
  final float[] values = new float[]{
      800.0f, 800.0f, 826.0602f, 1564.6177f, 1006.4021f, 869.64374f, 809.04175f, 1458.4027f, 852.4375f, 879.9881f,
      782.0135f, 829.91626f, 1862.7379f, 873.3065f, 1427.0167f, 1430.2573f, 1101.9182f, 1166.1411f, 1004.94086f,
      740.1837f, 865.7779f, 901.30756f, 691.9589f, 1674.3317f, 975.57794f, 1360.6948f, 755.89935f, 771.34845f,
      869.30835f, 1095.6376f, 906.3738f, 988.8938f, 835.76263f, 776.70294f, 875.6834f, 1070.8363f, 835.46124f,
      715.5161f, 755.64655f, 771.1005f, 764.50806f, 736.40924f, 884.8373f, 918.72284f, 893.98505f, 832.8749f,
      850.995f, 767.9733f, 848.3399f, 878.6838f, 906.1019f, 1403.8302f, 936.4296f, 846.2884f, 856.4901f, 1032.2576f,
      954.7542f, 1031.99f, 907.02155f, 1110.789f, 843.95215f, 1362.6506f, 884.8015f, 1684.2688f, 873.65204f, 855.7177f,
      996.56415f, 1061.6786f, 962.2358f, 1019.8985f, 1056.4193f, 1198.7231f, 1108.1361f, 1289.0095f,
      1069.4318f, 1001.13403f, 1030.4995f, 1734.2749f, 1063.2012f, 1447.3412f, 1234.2476f, 1144.3424f, 1049.7385f,
      811.9913f, 768.4231f, 1151.0692f, 877.0794f, 1146.4231f, 902.6157f, 1355.8434f, 897.39343f, 1260.1431f, 762.8625f,
      935.168f, 782.10785f, 996.2054f, 767.69214f, 1031.7415f, 775.9656f, 1374.9684f, 853.163f, 1456.6118f, 811.92523f,
      989.0328f, 744.7446f, 1166.4012f, 753.105f, 962.7312f, 780.272f,
      1000.0f, 1040.9456f, 1689.0128f, 1049.142f, 1073.4766f, 1007.36554f, 1545.7089f, 1016.9652f, 1077.6127f,
      1075.0896f, 953.9954f, 1022.7833f, 937.06195f, 1156.7448f, 849.8775f, 1066.208f, 904.34064f, 1240.5255f,
      1343.2325f, 1088.9431f, 1349.2544f, 1102.8667f, 939.2441f, 1109.8754f, 997.99457f, 1037.4495f, 1686.4197f,
      1074.007f, 1486.2013f, 1300.3022f, 1021.3345f, 1314.6195f, 792.32605f, 1233.4489f, 805.9301f, 1184.9207f,
      1127.231f, 1203.4656f, 1100.9048f, 1097.2112f, 1410.793f, 1033.4012f, 1283.166f, 1025.6333f, 1331.861f,
      1039.5005f, 1332.4684f, 1011.20544f, 1029.9952f, 1047.2129f, 1057.08f, 1064.9727f, 1082.7277f, 971.0508f,
      1320.6383f, 1070.1655f, 1089.6478f, 980.3866f, 1179.6959f, 959.2362f, 1092.417f, 987.0674f, 1103.4583f,
      950.1468f, 712.7746f, 846.2675f, 682.8855f, 1109.875f, 594.3817f, 870.1159f, 677.511f, 1410.2781f, 1219.4321f,
      979.306f, 1224.5016f, 1215.5898f, 716.6092f, 1301.0233f, 786.3633f, 989.9315f, 1609.0967f, 1023.2952f, 1367.6381f,
      1627.598f, 810.8894f, 1685.5001f, 545.9906f, 1870.061f, 555.476f, 1643.3408f, 943.4972f, 1667.4978f, 913.5611f,
      1218.5619f, 1273.7074f, 888.70526f, 1113.1141f, 864.5689f, 1308.582f, 785.07886f, 1363.6149f, 787.1253f,
      826.0392f, 1107.2438f, 872.6257f, 1188.3693f, 911.9568f, 794.0988f, 1299.0933f, 1212.9283f, 901.3273f, 723.5143f,
      1061.9734f, 602.97955f, 879.4061f, 724.2625f, 862.93134f, 1133.1351f, 948.65796f, 807.6017f, 914.525f, 1553.3485f,
      1208.4567f, 679.6193f, 645.1777f, 1120.0887f, 1649.5333f, 1433.3988f, 1598.1793f, 1192.5631f, 1022.85455f,
      1228.5024f, 1298.4158f, 1345.9644f, 1291.898f, 1306.4957f, 1287.7667f, 1631.5844f, 578.79596f, 1017.5732f,
      1091.2231f, 1199.6074f, 1044.3843f, 1183.2408f, 1289.0973f, 1360.0325f, 993.59125f, 1021.07117f, 1105.3834f,
      1601.8295f, 1200.5272f, 1600.7233f, 1317.4584f, 1304.3262f, 1544.1082f, 1488.7378f, 1224.8271f, 1421.6487f,
      1251.9062f, 1414.619f, 1350.1754f, 970.7283f, 1057.4272f, 1073.9673f, 996.4337f, 1743.9218f, 1044.5629f,
      1474.5911f, 1159.2788f, 1292.5428f, 1124.2014f, 1243.354f, 1051.809f, 1143.0784f, 1097.4907f, 1010.3703f,
      1326.8291f, 1179.8038f, 1281.6012f, 994.73126f, 1081.6504f, 1103.2397f, 1177.8584f, 1152.5477f, 1117.954f,
      1084.3325f, 1029.8025f, 1121.3854f, 1244.85f, 1077.2794f, 1098.5432f, 998.65076f, 1088.8076f, 1008.74554f,
      998.75397f, 1129.7233f, 1075.243f, 1141.5884f, 1037.3811f, 1099.1973f, 981.5773f, 1092.942f, 1072.2394f,
      1154.4156f, 1311.1786f, 1176.6052f, 1107.2202f, 1102.699f, 1285.0901f, 1217.5475f, 1283.957f, 1178.8302f,
      1301.7781f, 1119.2472f, 1403.3389f, 1156.6019f, 1429.5802f, 1137.8423f, 1124.9352f, 1256.4998f, 1217.8774f,
      1247.8909f, 1185.71f, 1345.7817f, 1250.1667f, 1390.754f, 1224.1162f, 1361.0802f, 1190.9337f, 1310.7971f,
      1466.2094f, 1366.4476f, 1314.8397f, 1522.0437f, 1193.5563f, 1321.375f, 1055.7837f, 1021.6387f, 1197.0084f,
      1131.532f, 1192.1443f, 1154.2896f, 1272.6771f, 1141.5146f, 1190.8961f, 1009.36316f, 1006.9138f, 1032.5999f,
      1137.3857f, 1030.0756f, 1005.25305f, 1030.0947f, 1112.7948f, 1113.3575f, 1153.9747f, 1069.6409f, 1016.13745f,
      994.9023f, 1032.1543f, 999.5864f, 994.75275f, 1029.057f
  };

  @Test
  public void testSame()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 5);
    for (int i = 0; i < 100; i++) {
      histogram.offer(100);
    }
    Assert.assertNull(histogram.snapshot(5));
  }

  @Test
  public void testSame2()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 5);
    for (int i = 0; i < 5; i++) {
      histogram.offer(100);
    }
    for (int i = 0; i < 20; i++) {
      histogram.offer(200);
    }
    FloatBitmaps bitmaps = histogram.snapshot(5);

    Assert.assertArrayEquals(new float[]{100, 200}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{25}, bitmaps.getSizes());
  }

  @Test
  public void testMinMax()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 0; i < 4; i++) {
      histogram.offer(100);
    }
    for (int i = 0; i < 6; i++) {
      histogram.offer(200);
    }
    histogram.offer(100);
    histogram.offer(200);
    histogram.offer(300);
    FloatBitmaps bitmaps = histogram.snapshot(3);

    Assert.assertArrayEquals(new float[]{100, 200, 300}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{5, 8}, bitmaps.getSizes());
  }

  @Test
  public void testMaxIncrease0()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 0; i < 10; i++) {
      histogram.offer(i);
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(10);

    Assert.assertArrayEquals(new float[]{0f, 1f, 2f, 3f, 4f, 6f, 8f, 9f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{2, 2, 2, 2, 4, 4, 4}, bitmaps.getSizes());

    // empty
    assertEquals(bitmaps.filterFor(Range.openClosed(4f, 4f)), new int[]{});
    assertEquals(bitmaps.filterFor(Range.closedOpen(4f, 4f)), new int[]{});

    // point
    assertEquals(bitmaps.filterFor(Range.closed(4f, 4f)), new int[]{8, 9, 10, 11});

    // lower ~
    assertEquals(bitmaps.filterFor(Range.downTo(6f, BoundType.CLOSED)), new int[]{12, 13, 14, 15, 16, 17, 18, 19});
    assertEquals(bitmaps.filterFor(Range.downTo(6f, BoundType.OPEN)), new int[]{12, 13, 14, 15, 16, 17, 18, 19});

    // ~ upper
    assertEquals(bitmaps.filterFor(Range.upTo(2f, BoundType.CLOSED)), new int[]{0, 1, 2, 3, 4, 5});
    assertEquals(bitmaps.filterFor(Range.upTo(2f, BoundType.OPEN)), new int[]{0, 1, 2, 3});

    // to max
    assertEquals(bitmaps.filterFor(Range.openClosed(8f, 9f)), new int[]{16, 17, 18, 19});
    assertEquals(bitmaps.filterFor(Range.closedOpen(8f, 9f)), new int[]{16, 17, 18, 19});
    assertEquals(bitmaps.filterFor(Range.openClosed(8f, 10f)), new int[]{16, 17, 18, 19});
    assertEquals(bitmaps.filterFor(Range.closedOpen(8f, 10f)), new int[]{16, 17, 18, 19});

    // over max
    assertEquals(bitmaps.filterFor(Range.downTo(9f, BoundType.CLOSED)), new int[]{16, 17, 18, 19});
    assertEquals(bitmaps.filterFor(Range.downTo(9f, BoundType.OPEN)), new int[]{});

    // under min
    assertEquals(bitmaps.filterFor(Range.upTo(0f, BoundType.CLOSED)), new int[]{0, 1});
    assertEquals(bitmaps.filterFor(Range.upTo(0f, BoundType.OPEN)), new int[]{});

    // range
    assertEquals(bitmaps.filterFor(Range.openClosed(1.5f, 2.5f)), new int[]{2, 3, 4, 5});
    assertEquals(bitmaps.filterFor(Range.openClosed(2f, 2.5f)), new int[]{4, 5});
    assertEquals(bitmaps.filterFor(Range.closed(1.5f, 3f)), new int[]{2, 3, 4, 5, 6, 7});
    assertEquals(bitmaps.filterFor(Range.openClosed(0.1f, 0.5f)), new int[]{0, 1});

    // oob
    assertEquals(bitmaps.filterFor(Range.upTo(-1f, BoundType.CLOSED)), new int[]{});
    assertEquals(bitmaps.filterFor(Range.downTo(10f, BoundType.CLOSED)), new int[]{});
  }

  private void assertEquals(ImmutableBitmap bitmap, int[] expected)
  {
    Assert.assertEquals("Length mismatch " + bitmap.toString(), expected.length, bitmap.size());
    IntIterator intIterator = bitmap.iterator();
    for (int e : expected) {
      Assert.assertEquals("Value mismatch " + bitmap.toString(), e, intIterator.next());
    }
  }

  @Test
  public void testMaxIncrease1()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 0; i < 10; i++) {
      histogram.offer(i);
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(5);

    Assert.assertArrayEquals(new float[]{0f, 2f, 4f, 6f, 8f, 9f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{4, 4, 4, 4, 4}, bitmaps.getSizes());
  }

  @Test
  public void testMaxIncrease2()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 0; i < 10; i++) {
      histogram.offer(i);
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(3);

    Assert.assertArrayEquals(new float[]{0f, 3f, 6f, 9f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{6, 6, 8}, bitmaps.getSizes());

    assertEquals(bitmaps.filterFor(Range.closed(0f, 0f)), new int[]{0, 1}); // return exactly
  }

  @Test
  public void testMinDecrease1()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 10; i > 0; i--) {
      histogram.offer(i);
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(5);

    Assert.assertArrayEquals(new float[]{1f, 3f, 5f, 7f, 9f, 10f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{4, 4, 4, 4, 4}, bitmaps.getSizes());
  }

  @Test
  public void testMinDecrease2()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 10);
    for (int i = 10; i > 0; i--) {
      histogram.offer(i);
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(3);

    Assert.assertArrayEquals(new float[]{1f, 4f, 7f, 10f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{6, 6, 8}, bitmaps.getSizes());
  }

  @Test
  public void testMore()
  {
    runTest(
        10,
        5,
        new float[]{545.9906f, 869.64374f, 1006.4021f, 1458.4027f, 1870.061f},
        new int[]{66, 63, 214, 28}
    );

    runTest(
        50,
        5,
        new float[]{545.9906f, 869.64374f, 1004.94086f, 1095.6376f, 1166.1411f, 1430.2573f, 1870.061f},
        new int[]{66, 61, 72, 46, 94, 32}
    );

    runTest(
        200,
        5,
        new float[]{545.9906f, 856.4901f, 1016.9652f, 1103.4583f, 1234.2476f, 1627.598f, 1870.061f},
        new int[]{61, 76, 72, 69, 79, 14}
    );

    runTest(
        200,
        10,
        new float[]{545.9906f, 800.0f, 879.9881f, 989.9315f, 1032.2576f, 1077.6127f, 1103.4583f, 1166.1411f, 1234.2476f, 1332.4684f, 1627.598f, 1870.061f},
        new int[]{38, 37, 38, 43, 33, 20, 36, 33, 37, 42, 14}
    );
  }

  private void runTest(int sample, int group, float[] breaks, int[] sizes)
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), sample);
    for (float value : values) {
      histogram.offer(value);
    }
    FloatBitmaps bitmaps = histogram.snapshot(group);

    Assert.assertArrayEquals(breaks, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(sizes, bitmaps.getSizes());

    Assert.assertEquals(545.9906f, bitmaps.getMin(), 0.001f);
    Assert.assertEquals(1870.061f, bitmaps.getMax(), 0.001f);
  }

  @Test
  public void testCompact1()
  {
    FloatHistogram histogram = new FloatHistogram(new ConciseBitmapFactory(), 100, 20, 10000);
    for (int i = 10000; i <= 100000; i++) {
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(5);
    Assert.assertArrayEquals(new float[]{10000f, 26411f, 45589f, 62411f, 77845f, 97825f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{16411, 19178, 16822, 15434, 19980, 2176}, bitmaps.getSizes());

    bitmaps = histogram.snapshot(10);

    Assert.assertArrayEquals(new float[]{10000f, 19801f, 31241f, 41281f, 50491f, 62411f, 69631f, 77845f, 87191f, 97825f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{9801, 11440, 10040, 9210, 11920, 7220, 8214, 9346, 10634, 2176}, bitmaps.getSizes());

    bitmaps = histogram.snapshot(20);

    Assert.assertArrayEquals(new float[]{10000f, 14527f, 19801f, 26411f, 31241f, 37495f, 41281f, 45589f, 50491f, 56067f, 62411f, 69631f, 77845f, 82367f, 87191f, 92337f, 97825f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{4527, 5274, 6610, 4830, 6254, 3786, 4308, 4902, 5576, 6344, 7220, 8214, 4522, 4824, 5146, 5488, 2176}, bitmaps.getSizes());
  }

  @Test
  public void testCompact2()
  {
    FloatHistogram histogram = new FloatHistogram(new RoaringBitmapFactory(), 1000, 20, 10000);
    for (int i = 100000; i >= 10000; i--) {
      histogram.offer(i);
    }
    FloatBitmaps bitmaps = histogram.snapshot(5);

    Assert.assertArrayEquals(new float[]{10000f, 29317f, 47258f, 64306f, 80129f, 97450f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{19317, 17941, 17048, 15823, 17321, 2551}, bitmaps.getSizes());

    bitmaps = histogram.snapshot(10);

    Assert.assertArrayEquals(new float[]{10000f, 22071f, 32683f, 41850f, 52162f, 64306f, 75844f, 87806f, 97450f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{12071, 10612, 9167, 10312, 12144, 11538, 11962, 9644, 2551}, bitmaps.getSizes());

    bitmaps = histogram.snapshot(20);

    Assert.assertArrayEquals(new float[]{10000f, 14082f, 22071f, 29317f, 32683f, 38942f, 41850f, 47258f, 52162f, 56611f, 64306f, 70636f, 75844f, 80129f, 87806f, 92875f, 97450f, 100000f}, bitmaps.breaks(), 0.001f);
    Assert.assertArrayEquals(new int[]{4082, 7989, 7246, 3366, 6259, 2908, 5408, 4904, 4449, 7695, 6330, 5208, 4285, 7677, 5069, 4575, 2551}, bitmaps.getSizes());
  }
}