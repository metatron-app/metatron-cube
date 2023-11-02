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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Ints;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.ops.transforms.Transforms;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class Paragraph2VecFunctions
{
  private static final Logger LOG = new Logger(Paragraph2VecFunctions.class);

  static void register(String name, ParagraphVectors p2Vec)
  {
    Parser.register(new VectorFunc(name, p2Vec));
    Parser.register(new VectorBatchFunc(name, p2Vec));
    Parser.register(new SimilarityFunc(name, p2Vec));
    Parser.register(new NerestFunc(name, p2Vec));
    Parser.register(new PredictFunc(name, p2Vec));
  }

  static List<String> predict(ParagraphVectors model, String text, int n)
  {
    INDArray vector = model.inferVector(text);
    INDArray syn0 = model.getLookupTable().getWeights();
    INDArray similarities = Transforms.allCosineSimilarities(syn0, vector, 1);

    MinMaxPriorityQueue<DocScore> queue = MinMaxPriorityQueue.expectedSize(n).maximumSize(n).create();
    for (int i = 0; i < similarities.rows(); i++) {
      queue.add(new DocScore(i, similarities.getFloat(i, 0)));
    }
    List<String> docs = Lists.newArrayList();
    VocabCache<VocabWord> vocab = model.getLookupTable().getVocabCache();
    while (!queue.isEmpty()) {
      docs.add(vocab.elementAtIndex(queue.poll().doc).getLabel());
    }
    return docs;
  }

  private static class VectorFunc extends Function.Factory.Simple
  {
    private final ParagraphVectors model;

    private VectorFunc(String name, ParagraphVectors model)
    {
      super("p2v_" + name + "_vector", ValueDesc.FLOAT_ARRAY);
      this.model = model;
    }

    @Override
    protected float[] _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String text = Evals.evalString(args.get(0), bindings);
      return model.inferVector(text).data().asFloat();
    }
  }

  private static abstract class BatchFunction extends Function.WithType implements Closeable
  {
    BatchFunction(ValueDesc type) {super(type);}
  }

  // this is for ingestion, not for query
  private static class VectorBatchFunc extends Function.Factory.WithType
  {
    private final ParagraphVectors model;

    private VectorBatchFunc(String name, ParagraphVectors model)
    {
      super("p2v_" + name + "_batch", ValueDesc.FLOAT_ARRAY);
      this.model = model;
    }

    @Override
    public BatchFunction create(List<Expr> args, TypeResolver resolver)
    {
      String identity = Long.toHexString(System.identityHashCode(this));
      BatchFunction fn = new BatchFunction(ValueDesc.FLOAT_ARRAY)
      {
        private final BatchInferer inferer = new BatchInferer(model);

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String text = Evals.evalString(args.get(0), bindings);
          return ExprEval.of(inferer.inferVector(text).data().asFloat(), ValueDesc.FLOAT_ARRAY);
        }

        @Override
        public void close() throws IOException
        {
          inferer.close();
          LOG.info("batch function %s[%s] closed", name(), identity);
        }
      };
      LOG.info("batch function %s[%s] created", name(), identity);
      return fn;
    }
  }

  private static class SimilarityFunc extends Function.Factory.Simple
  {
    private final ParagraphVectors model;

    private SimilarityFunc(String name, ParagraphVectors model)
    {
      super("p2v_" + name + "_similarity", ValueDesc.DOUBLE);
      this.model = model;
    }

    @Override
    protected Double _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String label1 = Evals.evalString(args.get(0), bindings);
      String label2 = Evals.evalString(args.get(1), bindings);
      return model.similarity(label1, label2);
    }
  }

  private static class NerestFunc extends Function.Factory.WithType.Simple
  {
    private final ParagraphVectors model;

    private NerestFunc(String name, ParagraphVectors model)
    {
      super("p2v_" + name + "_nearest", ValueDesc.STRING_ARRAY);
      this.model = model;
    }

    @Override
    protected List<String> _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String text = Evals.evalString(args.get(0), bindings);
      int n = Evals.evalInt(args.get(1), bindings);
      return ImmutableList.copyOf(model.nearestLabels(text, n));
    }
  }

  private static class PredictFunc extends Function.Factory.WithType.Simple
  {
    private final ParagraphVectors model;

    private PredictFunc(String name, ParagraphVectors model)
    {
      super("p2v_" + name + "_predict", ValueDesc.STRING_ARRAY);
      this.model = model;
    }

    @Override
    protected List<String> _evaluate(List<Expr> args, Expr.NumericBinding bindings)
    {
      String text = Evals.evalString(args.get(0), bindings);
      int n = args.size() > 1 ? Evals.evalInt(args.get(1), bindings) : 1;
      return predict(model, text, n);
    }
  }

  private static class DocScore implements Comparable<DocScore>
  {
    private final int doc;
    private final float score;

    private DocScore(int doc, float score)
    {
      this.doc = doc;
      this.score = score;
    }

    @Override
    public int compareTo(DocScore o)
    {
      int compare = Float.compare(score, o.score);
      return compare == 0 ? Ints.compare(doc, o.doc) : -compare;
    }
  }
}
