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

package io.druid.query.aggregation.hll;

import com.yahoo.sketches.hll.HllSketch;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

// This is used for generating test data for HllSketchAggregatorTest
class GenerateTestData
{

  public static void main(String[] args) throws Exception
  {
    generateSketches();
  }

  private static void generateSketches() throws Exception
  {
    int lgK = 12;
    String date = "20170101";
    Path rawPath = FileSystems.getDefault().getPath("hll_raw.tsv");
    Path sketchPath = FileSystems.getDefault().getPath("hll_sketches.tsv");
    try (BufferedWriter out1 = Files.newBufferedWriter(rawPath, StandardCharsets.UTF_8)) {
      try (BufferedWriter out2 = Files.newBufferedWriter(sketchPath, StandardCharsets.UTF_8)) {
        Random rand = ThreadLocalRandom.current();
        int key = 0;
        for (int i = 0; i < 100; i++) {
          HllSketch sketch = new HllSketch(lgK);
          String dimension = Integer.toString(rand.nextInt(10) + 1);
          writeRawRecord(out1, date, dimension, key);
          sketch.update(key++);
          writeRawRecord(out1, date, dimension, key);
          sketch.update(key++);
          writeSketchRecord(out2, date, dimension, sketch);
        }
      }
    }
  }

  private static void writeRawRecord(BufferedWriter out, String date, String dimension, int id) throws Exception
  {
    out.write(date);
    out.write("\t");
    out.write(dimension);
    out.write("\t");
    out.write(Integer.toString(id));
    out.newLine();
  }

  private static void writeSketchRecord(BufferedWriter out, String date, String dimension, HllSketch sketch) throws Exception
  {
    out.write(date);
    out.write("\t");
    out.write(dimension);
    out.write("\t");
    out.write(Base64.encodeBase64String(sketch.toCompactByteArray()));
    out.newLine();
  }

}
