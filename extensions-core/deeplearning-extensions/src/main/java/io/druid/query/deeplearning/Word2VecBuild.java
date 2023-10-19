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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.deeplearning4j.models.embeddings.loader.VectorsConfiguration;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.IOException;

@JsonTypeName("w2v_build")
public class Word2VecBuild extends VectorsConfiguration implements ModelConf
{
  @JsonProperty
  public String source;

  public void build(String name, ClassLoader loader) throws IOException
  {
    Preconditions.checkNotNull(source, "source?");
    SentenceIterator sequences = new BasicLineIterator(loader.getResource(source).getFile());
    TokenizerFactory tokenizer = new DefaultTokenizerFactory();
    tokenizer.setTokenPreProcessor(new CommonPreprocessor());

    Word2Vec word2Vec = new Word2Vec.Builder(this).iterate(sequences).tokenizerFactory(tokenizer).build();
    word2Vec.fit();   // seemed to be done in async
    DL4JFunctions.register(name, word2Vec);
  }
}
