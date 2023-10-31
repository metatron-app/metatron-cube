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

package io.druid.query.deeplearning;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "w2v_build", value = Word2VecBuild.class),
    @JsonSubTypes.Type(name = "w2v_load", value = Word2VecLoad.class),
    @JsonSubTypes.Type(name = "p2v_build", value = Paragraph2VecBuild.class),
    @JsonSubTypes.Type(name = "p2v_load", value = Paragraph2VecLoad.class)
})
public interface ModelConf
{
  void build(String name, ClassLoader loader) throws IOException;

  static final Matcher MATCHER = Pattern.compile("[\\d.:,\"'()\\[\\]|/?!;]+").matcher("");

  static TokenizerFactory tokenizer()
  {
    TokenizerFactory tokenizer = new DefaultTokenizerFactory();
    tokenizer.setTokenPreProcessor(t -> {
      Matcher reset = MATCHER.reset(t);
      return reset.matches() ? reset.replaceAll("").toLowerCase() : t.toLowerCase();
    });
    return tokenizer;
  }
}
