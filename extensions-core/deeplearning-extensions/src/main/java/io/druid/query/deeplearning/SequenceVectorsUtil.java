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

import com.google.common.primitives.Ints;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import org.deeplearning4j.models.embeddings.inmemory.InMemoryLookupTable;
import org.deeplearning4j.models.embeddings.loader.VectorsConfiguration;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.sequencevectors.sequence.SequenceElement;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.AbstractCache;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SequenceVectorsUtil
{
  private static final int BUFFER_SIZE = 65536;

  public static <T extends SequenceElement> File w2v_write(Word2Vec word2Vec, File file) throws IOException
  {
    try (FileOutputStream fos = new FileOutputStream(file)) {
      w2v_write(word2Vec, fos);
    }
    return file;
  }

  @SuppressWarnings("unchecked")
  private static void w2v_write(Word2Vec word2Vec, FileOutputStream target) throws IOException
  {
    BytesOutputStream output = new BytesOutputStream();

    output.writeVarSizeUTF(word2Vec.getConfiguration().toJson());
    InMemoryLookupTable<VocabWord> lookupTable = (InMemoryLookupTable<VocabWord>) word2Vec.lookupTable();
    VocabCache<VocabWord> vocabCache = lookupTable.getVocabCache();

    output.writeUnsignedVarInt(vocabCache.numWords());
    for (VocabWord token : vocabCache.vocabWords()) {
      output.writeUnsignedVarInt(token.getIndex());
      output.writeBoolean(token.isLabel());
      output.writeVarSizeUTF(token.getWord());

      List<Byte> codes = token.getCodes();
      int codeLength = token.getCodeLength();
      output.writeUnsignedVarInt(codeLength);
      for (int i = 0; i < codeLength; i++) {
        output.write(i < codes.size() ? codes.get(i) : 0);
      }
      List<Integer> points = token.getPoints();
      output.writeUnsignedVarInt(points.size());
      for (Integer point : points) {
        output.writeVarInt(point);
      }
      output.writeDouble(token.getElementFrequency());
      output.writeUnsignedVarInt(vocabCache.docAppearedIn(token.getWord()));

      if (output.size() >= BUFFER_SIZE) {
        target.write(Ints.toByteArray(output.size()));
        target.write(output.unwrap(), 0, output.size());
        output.clear();
      }
    }
    if (output.size() > 0) {
      target.write(Ints.toByteArray(output.size()));
      target.write(output.unwrap(), 0, output.size());
    }

    INDArray syn0 = lookupTable.getSyn0();
    INDArray syn1 = lookupTable.getSyn1();
    INDArray syn1x = lookupTable.getSyn1Neg();

    DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(target, BUFFER_SIZE));
    Nd4j.write(syn0, dout);
    Nd4j.write(syn1, dout);
    if (syn1x != null) {
      Nd4j.write(syn1x, dout);
    }
    dout.flush();
  }

  public static Word2Vec w2v_load(File file) throws IOException
  {
    return w2v_load(new DataInputStream(new FileInputStream(file)))
        .useAdaGrad(false)
        .useHierarchicSoftmax(false)
        .resetModel(false)
        .build();
  }

  private static Word2Vec.Builder w2v_load(DataInputStream source) throws IOException
  {
    byte[] bytes = new byte[source.readInt()];
    source.readFully(bytes);
    BytesInputStream input = new BytesInputStream(bytes);
    String json = input.readVarSizeUTF();
    VectorsConfiguration config = VectorsConfiguration.fromJson(json);
    int numWords = input.readUnsignedVarInt();
    int layer = config.getLayersSize();

    AbstractCache<VocabWord> vocabCache = new AbstractCache<>();
    for (int i = 0; i < numWords; i++) {
      if (input.available() == 0) {
        bytes = new byte[source.readInt()];
        source.readFully(bytes);
        input = new BytesInputStream(bytes);
      }
      int index = input.readUnsignedVarInt();
      boolean isLabel = input.readBoolean();
      String word = input.readVarSizeUTF();
      Byte[] codes = new Byte[input.readUnsignedVarInt()];
      for (int x = 0; x < codes.length; x++) {
        codes[x] = (byte) input.read();
      }
      Integer[] points = new Integer[input.readUnsignedVarInt()];
      for (int x = 0; x < points.length; x++) {
        points[x] = input.readVarInt();
      }
      double wordFrequency = input.readDouble();
      int docFrequency = input.readUnsignedVarInt();

      VocabWord token = new VocabWord(wordFrequency, word);
      token.setIndex(index);
      token.markAsLabel(isLabel);
      token.setCodes(Arrays.asList(codes));
      token.setCodeLength((short) codes.length);
      token.setPoints(Arrays.asList(points));
      token.incrementSequencesCount(docFrequency);

      vocabCache.addToken(token);
      vocabCache.addWordToIndex(index, word);
    }
    INDArray syn0 = Nd4j.read(source);
    INDArray syn1 = Nd4j.read(source);
    INDArray syn1x = source.available() > 0 ? Nd4j.read(source) : null;

    InMemoryLookupTable<VocabWord> lookupTable = new InMemoryLookupTable.Builder<VocabWord>().cache(vocabCache)
                                                                                             .vectorLength(layer)
                                                                                             .useHierarchicSoftmax(false)
                                                                                             .useAdaGrad(false)
                                                                                             .build();
    lookupTable.setSyn0(syn0);
    lookupTable.setSyn1(syn1);
    lookupTable.setSyn1Neg(syn1x);

    return new Word2Vec.Builder(config).lookupTable(lookupTable)
                                       .vocabCache(vocabCache)
                                       .layerSize(layer);
  }

  public static ParagraphVectors p2v_load(File file) throws IOException
  {
    DataInputStream source = new DataInputStream(new BufferedInputStream(new FileInputStream(file), BUFFER_SIZE));
    Word2Vec w2v = w2v_load(source).build();
    ParagraphVectors vectors = new ParagraphVectors.Builder(w2v.getConfiguration())
        .vocabCache(w2v.getVocab())
        .lookupTable(w2v.getLookupTable())
        .resetModel(false)
        .build();
    vectors.extractLabels();
    return vectors;
  }
}
