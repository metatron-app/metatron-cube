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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.AbstractCache;
import org.deeplearning4j.text.documentiterator.LabelsSource;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;

import java.io.IOException;

@JsonTypeName("p2v_build")
public class Paragraph2VecBuild extends Word2VecBuild implements ModelConf
{
  @Override
  public void build(String name, ClassLoader loader) throws IOException
  {
    Preconditions.checkNotNull(source, "source?");
    SentenceIterator sequences = new BasicLineIterator(loader.getResource(source).getFile());

    ParagraphVectors p2vec = new ParagraphVectors.Builder(this)
        .labelsSource(new LabelsSource(""))
        .iterate(sequences)
        .tokenizerFactory(ModelConf.tokenizer())
        .vocabCache(new AbstractCache<>())
        .trainWordVectors(false)
        .build();

    p2vec.fit();   // seemed to be done in async
    Paragraph2VecFunctions.register(name, p2vec);
  }
}
