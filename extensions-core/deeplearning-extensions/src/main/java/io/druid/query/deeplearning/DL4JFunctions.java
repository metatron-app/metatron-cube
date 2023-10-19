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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.data.ValueDesc;
import io.druid.guice.annotations.Json;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import org.deeplearning4j.models.word2vec.Word2Vec;

import java.util.List;

public class DL4JFunctions implements Function.Library
{
  @Inject
  public static Injector injector;

  @Inject
  public static @Json
  ObjectMapper jsonMapper;


  static void register(String name, Word2Vec word2Vec)
  {
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
