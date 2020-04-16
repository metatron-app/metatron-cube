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

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.js.JavaScriptConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CascadeExtractionFnTest
{
  private static final String[] paths = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private final String regex = "/([^/]+)/";
  private final String function = "function(str) { return \"the \".concat(str) }";
  private final RegexDimExtractionFn regexDimExtractionFn = new RegexDimExtractionFn(regex, false, null);
  private final JavaScriptExtractionFn javascriptExtractionFn = new JavaScriptExtractionFn(
      function,
      true,
      JavaScriptConfig.getDefault()
  );
  private final SubstringDimExtractionFn substringDimExtractionFn = new SubstringDimExtractionFn(0, 7);
  private final String regexDimExtractionFnJson = "{ \"type\" : \"regex\", \"expr\" : \"/([^/]+)/\" , " +
                                                  "\"replaceMissingValue\": false, \"replaceMissingValueWith\": null}";
  private final String javascriptExtractionFnJson =
      "{ \"type\" : \"javascript\", \"function\" : \"function(str) { return \\\"the \\\".concat(str) }\" }";
  private final String substringDimExtractionFnJson = "{ \"type\" : \"substring\", \"index\" : 0, \"length\" : 7 }";

  @Test
  public void testRegexAndJavascriptAndSubstring()
  {
    CascadeExtractionFn cascadeExtractionFn = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn, substringDimExtractionFn)
    );

    Set<String> extracted = Sets.newLinkedHashSet();
    for (String path : paths) {
      extracted.add(cascadeExtractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(ImmutableList.of("the dru", "the das"));
    Assert.assertEquals(expected, extracted);
  }

  @Test
  public void testGetCacheKey()
  {
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(
        Arrays.asList(javascriptExtractionFn, regexDimExtractionFn)
    );

    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn)
    );

    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn, substringDimExtractionFn)
    );


    Assert.assertFalse(Arrays.equals(cascadeExtractionFn1.getCacheKey(), cascadeExtractionFn2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(cascadeExtractionFn1.getCacheKey(), cascadeExtractionFn3.getCacheKey()));
    Assert.assertFalse(Arrays.equals(cascadeExtractionFn2.getCacheKey(), cascadeExtractionFn3.getCacheKey()));
  }

  @Test
  public void testHashCode()
  {
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(
        Arrays.asList(javascriptExtractionFn, regexDimExtractionFn)
    );

    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn)
    );

    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn, substringDimExtractionFn)
    );

    Assert.assertNotEquals(cascadeExtractionFn1.hashCode(), cascadeExtractionFn2.hashCode());
    Assert.assertNotEquals(cascadeExtractionFn1.hashCode(), cascadeExtractionFn3.hashCode());
    Assert.assertNotEquals(cascadeExtractionFn2.hashCode(), cascadeExtractionFn3.hashCode());
  }

  @Test
  public void testPreservesOrdering()
  {
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(Arrays.asList(substringDimExtractionFn));

    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, substringDimExtractionFn)
    );

    ExtractionFn[] fns3 = new ExtractionFn[2];
    fns3[0] = substringDimExtractionFn;
    fns3[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(
        Arrays.asList(substringDimExtractionFn, javascriptExtractionFn)
    );

    Assert.assertTrue(cascadeExtractionFn1.preservesOrdering());
    Assert.assertFalse(cascadeExtractionFn2.preservesOrdering());
    Assert.assertFalse(cascadeExtractionFn3.preservesOrdering());
  }

  @Test
  public void testGetExtractionType()
  {
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(Arrays.asList(javascriptExtractionFn));

    ExtractionFn[] fns2 = new ExtractionFn[2];
    fns2[0] = regexDimExtractionFn;
    fns2[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(
        Arrays.asList(regexDimExtractionFn, javascriptExtractionFn)
    );

    Assert.assertTrue(cascadeExtractionFn1.isOneToOne());
    Assert.assertFalse(cascadeExtractionFn2.isOneToOne());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            JavaScriptConfig.getDefault()
        )
    );
    final String json = "{\"type\" : \"cascade\", \"extractionFns\": ["
                        +
                        regexDimExtractionFnJson
                        + ","
                        + javascriptExtractionFnJson
                        + ","
                        + substringDimExtractionFnJson
                        + "]}";

    CascadeExtractionFn cascadeExtractionFn = (CascadeExtractionFn) objectMapper.readValue(json, ExtractionFn.class);
    RegexDimExtractionFn regexDimExtractionFn =
        (RegexDimExtractionFn) objectMapper.readValue(regexDimExtractionFnJson, ExtractionFn.class);
    JavaScriptExtractionFn javascriptExtractionFn =
        (JavaScriptExtractionFn) objectMapper.readValue(javascriptExtractionFnJson, ExtractionFn.class);
    SubstringDimExtractionFn substringDimExtractionFn =
        (SubstringDimExtractionFn) objectMapper.readValue(substringDimExtractionFnJson, ExtractionFn.class);

    final List<ExtractionFn> extractionFns = cascadeExtractionFn.getExtractionFns();
    Assert.assertEquals(regexDimExtractionFn, extractionFns.get(0));
    Assert.assertEquals(javascriptExtractionFn, extractionFns.get(1));
    Assert.assertEquals(substringDimExtractionFn, extractionFns.get(2));

    Assert.assertEquals(
        cascadeExtractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(cascadeExtractionFn),
            ExtractionFn.class
        )
    );
  }
}
