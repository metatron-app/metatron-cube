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

import io.druid.java.util.common.guava.CloseQuietly;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.deeplearning4j.models.embeddings.inmemory.InMemoryLookupTable;
import org.deeplearning4j.models.embeddings.loader.VectorsConfiguration;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.api.ops.OpContext;
import org.nd4j.linalg.api.ops.impl.nlp.SkipGramRound;
import org.nd4j.linalg.api.ops.impl.scalar.ScalarDivision;
import org.nd4j.linalg.api.ops.impl.scalar.ScalarSubtraction;
import org.nd4j.linalg.api.rng.Random;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.ops.transforms.Transforms;
import org.nd4j.nativeblas.NativeOps;
import org.nd4j.nativeblas.NativeOpsHolder;
import org.nd4j.nativeblas.OpaqueContext;
import org.nd4j.nativeblas.OpaqueRandomGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// single-threded
public class BatchInferer
{
  private final ParagraphVectors vectors;
  private final VocabCache<VocabWord> vocab;
  private final InMemoryLookupTable<VocabWord> lookup;

  private final long seed;
  private final int iterations;
  private final double learningRate;
  private final double minLearningRate;

  private final Random r;

  private final INDArray target = Nd4j.scalar(-1);
  private final INDArray ngs = Nd4j.scalar(-1);

  private final INDArray syn0;
  private final INDArray syn1;
  private final INDArray syn1Neg;
  private final INDArray expTable;
  private final INDArray negTable;

  private final INDArray alpha = Nd4j.scalar(-1D);
  private final INDArray random = Nd4j.scalar(-1L);
  private long nr;

  private final INDArray ret;
  private final ScalarSubtraction subtraction;
  private final ScalarDivision division;

  private final Int2ObjectMap<ExecContext> cache = new Int2ObjectOpenHashMap<>();

  public BatchInferer(ParagraphVectors vectors)
  {
    this.vectors = vectors;
    this.vocab = vectors.getVocab();
    this.lookup = (InMemoryLookupTable<VocabWord>) vectors.getLookupTable();

    VectorsConfiguration configuration = vectors.getConfiguration();
    int layer = configuration.getLayersSize();

    this.seed = configuration.getSeed();
    this.iterations = configuration.getIterations();
    this.learningRate = configuration.getLearningRate();
    this.minLearningRate = configuration.getMinLearningRate();

    this.r = Nd4j.getRandomFactory().getNewRandomInstance(seed, layer + 1);
    this.syn0 = lookup.getSyn0();
    this.syn1 = lookup.getSyn1();
    this.syn1Neg = Nd4j.empty(syn1.dataType());
    this.expTable = Nd4j.create(lookup.getExpTable(), new long[]{lookup.getExpTable().length}, syn0.dataType());
    this.negTable = Nd4j.empty(syn1.dataType());
    this.ret = Nd4j.rand(r, 1, layer);
    this.subtraction = new ScalarSubtraction(ret, null, ret, 0.5);
    this.division = new ScalarDivision(ret, null, ret, layer);
    this.nr = seed;
  }

  public INDArray inferVector(String text)
  {
    INDArray vector = lookup.vector(text);   // label ?
    if (vector != null) {
      return vector.reshape(1, vector.length());
    }
    r.setSeed(seed * text.hashCode());
    Nd4j.rand(ret, r);
    Nd4j.getExecutioner().exec(subtraction);
    Nd4j.getExecutioner().exec(division);

    List<VocabWord> document = new ArrayList<>();
    Tokenizer tokenizer = vectors.getTokenizerFactory().create(text);
    for (String token : tokenizer.getTokens()) {
      VocabWord word = vocab.wordFor(token);
      if (word != null) {
        document.add(word);
      }
    }

    double currentRate = learningRate;
    for (int iter = 0; iter < iterations; iter++) {
      for (VocabWord word : document) {
        int length = word.getCodeLength();
        int[] indices = new int[length];
        byte[] codes = new byte[length];
        for (int i = 0; i < length; i++) {
          indices[i] = word.getPoints().get(i);
          codes[i] = word.getCodes().get(i);
        }
        ExecContext executor = cache.get(length);
        if (executor != null) {
          executor.context.getInputArray(2).data().setData(indices);
          executor.context.getInputArray(3).data().setData(codes);
        } else {
          SkipGramRound op = new SkipGramRound(
              target, ngs, syn0, syn1, syn1Neg, expTable, negTable, 0,
              Nd4j.createFromArray(indices), Nd4j.createFromArray(codes), alpha, random, ret, false, 1
          );
          OpContext context = Nd4j.getExecutioner().buildContext();
          context.markInplace(op.isInplaceCall());
          context.setInputArrays(op.inputArguments());
          context.setBArguments(op.bArgs());
          context.setIArguments(op.iArgs());
          cache.put(length, executor = new ExecContext(op, context));
        }
        alpha.putScalar(0, currentRate);
        random.putScalar(0, nr = Math.abs(nr * 25214903917L + 11));

        executor.execute();
      }
      currentRate = (currentRate - minLearningRate) / (iterations - iter) + minLearningRate;
    }
    return ret;   // do dup(), if needed
  }

  private static class ExecContext
  {
    private final SkipGramRound op;
    private final OpContext context;
    private final NativeOps nativeOps;
    private final OpaqueContext pointer;
    private final OpaqueRandomGenerator generator;

    private ExecContext(SkipGramRound op, OpContext context)
    {
      this.op = op;
      this.context = context;
      this.pointer = (OpaqueContext) context.contextPointer();
      this.nativeOps = NativeOpsHolder.getInstance().getDeviceNativeOps();
      this.generator = nativeOps.getGraphContextRandomGenerator(pointer);
    }

    private void execute()
    {
      Random random = Nd4j.getRandom();   // thread-local
      try {
        // transferring rng state
        nativeOps.setRandomGeneratorStates(generator, random.rootState(), random.nodeState());

        //      executioner.exec(op, context);    // skip profiling
        int status = nativeOps.execCustomOp2(null, op.opHash(), pointer);
        if (status != 0) {
          throw new RuntimeException(String.format(
              "Op with name %s and op type [%s] execution failed with message %s",
              op.getOwnName(),
              op.opName(),
              nativeOps.lastErrorMessage()
          ));
        }

        // pulling states back
        random.setStates(
            nativeOps.getRandomGeneratorRootState(generator),
            nativeOps.getRandomGeneratorNodeState(generator)
        );
      }
      catch (RuntimeException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RuntimeException("Op [" + op.opName() + "] execution failed", e);
      }
    }
  }

  public double similarity(String text1, String text2)
  {
    return Transforms.cosineSim(inferVector(text1), inferVector(text1));
  }

  public Collection<String> nearestLabels(String text, int n)
  {
    return vectors.nearestLabels(inferVector(text), n);
  }

  public void close()
  {
    target.close();
    ngs.close();
    syn1Neg.close();
    expTable.close();
    negTable.close();
    alpha.close();
    random.close();

    ret.close();
    for (ExecContext pair : cache.values()) {
      pair.op.clearArrays();
      CloseQuietly.close(pair.context);   // no-op
    }
    cache.clear();
  }
}
