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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import org.deeplearning4j.models.embeddings.loader.VectorsConfiguration;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import java.io.IOException;
import java.util.List;

public class Word2VecConf extends VectorsConfiguration
{
  @JsonProperty
  public String source;

  void build(String name, String property, ClassLoader loader) throws IOException
  {
    Preconditions.checkNotNull(source, "source?");
    SentenceIterator sequences = new BasicLineIterator(loader.getResource(source).getFile());
    TokenizerFactory tokenizer = new DefaultTokenizerFactory();
    tokenizer.setTokenPreProcessor(new CommonPreprocessor());
    Word2Vec word2Vec = new Word2Vec.Builder(this).iterate(sequences).tokenizerFactory(tokenizer).build();

    // seemed to be done in async
    word2Vec.fit();

    Parser.register(new VectorFunc(name, word2Vec));
    Parser.register(new SimilarityFunc(name, word2Vec));
    Parser.register(new NerestFunc(name, word2Vec));
  }

  private static class VectorFunc extends Function.Factory.WithType
  {
    private final Word2Vec model;

    private VectorFunc(String name, Word2Vec model)
    {
      super("w2v_" + name + "_vector", ValueDesc.DOUBLE_ARRAY);
      this.model = model;
    }

    @Override
    protected double[] _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String word = Evals.evalString(args.get(0), bindings);
      return model.getWordVector(word);
    }
  }

  private static class SimilarityFunc extends Function.Factory.WithType
  {
    private final Word2Vec model;

    private SimilarityFunc(String name, Word2Vec model)
    {
      super("w2v_" + name + "_similarity", ValueDesc.DOUBLE);
      this.model = model;
    }

    @Override
    protected Double _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String word1 = Evals.evalString(args.get(0), bindings);
      String word2 = Evals.evalString(args.get(1), bindings);
      return model.similarity(word1, word2);
    }
  }

  private static class NerestFunc extends Function.Factory.WithType
  {
    private final Word2Vec model;

    private NerestFunc(String name, Word2Vec model)
    {
      super("w2v_" + name + "_nearest", ValueDesc.STRING_ARRAY);
      this.model = model;
    }

    @Override
    protected List<String> _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String word = Evals.evalString(args.get(0), bindings);
      int n = Evals.evalInt(args.get(1), bindings);
      return ImmutableList.copyOf(model.wordsNearest(word, n));
    }
  }
}
