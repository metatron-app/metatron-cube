package io.druid.query.deeplearning;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import io.druid.java.util.common.VectorUtils;
import org.deeplearning4j.models.embeddings.WeightLookupTable;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.paragraphvectors.ParagraphVectors;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.AbstractCache;
import org.deeplearning4j.text.documentiterator.LabelsSource;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.junit.Test;
import org.nd4j.common.io.ClassPathResource;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.ops.transforms.Transforms;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * This is example code for dl4j ParagraphVectors implementation. In this example we build distributed representation of all sentences present in training corpus.
 * However, you still use it for training on labelled documents, using sets of LabelledDocument and LabelAwareIterator implementation.
 * <p>
 * *************************************************************************************************
 * PLEASE NOTE: THIS EXAMPLE REQUIRES DL4J/ND4J VERSIONS >= rc3.8 TO COMPILE SUCCESSFULLY
 * *************************************************************************************************
 *
 * @author raver119@gmail.com
 */
public class ParagraphVectorsTest
{
  @Test
  public void load() throws Exception
  {
    ParagraphVectors p2Vec = SequenceVectorsUtil.p2v_load(new ClassPathResource("/p2v_model.bin").getFile());
    p2Vec.setTokenizerFactory(ModelConf.tokenizer());

    long p = System.currentTimeMillis();
    System.out.println(p2Vec.predictSeveral("This is my", 3));
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - p);

    p = System.currentTimeMillis();
    System.out.println(Paragraph2VecFunctions.predict(p2Vec, "This is my", 3));
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - p);

    INDArray syn0 = p2Vec.lookupTable().getWeights();
    INDArray r0 = syn0.getRow(0, false);
    p = System.currentTimeMillis();
    INDArray similarities = Transforms.allCosineSimilarities(syn0, r0, 1);
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - p);
    p = System.currentTimeMillis();
    double[] ss = new double[syn0.rows()];
    for (int i = 0; i < syn0.rows(); i++) {
      ss[i]= Transforms.cosineSim(r0, syn0.getRow(i, false));
    }
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - p);

    for (int i = 0; i < syn0.rows(); i++) {
      Preconditions.checkArgument(similarities.getDouble(i, 0) == ss[i]);
    }

    SentenceIterator find = new BasicLineIterator(new ClassPathResource("/raw_sentences.txt").getFile());
    List<String> batch = Lists.newArrayList();
    for (int i = 0; i < 10000; i++) {
      batch.add(find.nextSentence());
    }

    String text = "We made it";
    String text10000 = "But I was home now .";
    // I was on time, But she does now, But where was home?,  Now I know more, She 's home now
    // I could go home now, I own my own home now,  Now I know more, I was on time, But now I do

    BatchInferer inferer = new BatchInferer(p2Vec);

    nearestLabels(p2Vec, inferer, text, 5);
    nearestLabels(p2Vec, inferer, text10000, 5);

    WeightLookupTable lookupTable = p2Vec.lookupTable();
    INDArray v1 = lookupTable.vector("DOC_9835");
    System.out.println(Arrays.toString(v1.shape()));
    INDArray v2 = p2Vec.inferVector("This is my house .");
    System.out.println(Arrays.toString(v2.shape()));

    System.out.println(p2Vec.nearestLabels(v1.reshape(1, v1.length()), 3));
    System.out.println(p2Vec.nearestLabels(v2, 3));
    long s = System.currentTimeMillis();
    for (String x : batch) {
      inferer.inferVector(x).data().asFloat();
//      System.out.println( x + " = " + Arrays.toString(Arrays.copyOf(inferer.inferVector(x).data().asFloat(), 5)));
    }
//    List<INDArray> vectors = SequenceVectorsUtil.inferVectors(batch, p2Vec);
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - s);

    s = System.currentTimeMillis();
    for (String x : batch) {
      p2Vec.inferVector(x).data().asFloat();
//      System.out.println( x + " = " + Arrays.toString(Arrays.copyOf(p2Vec.inferVector(x).data().asFloat(), 5)));
    }
    System.out.printf("took %,d msec%n", System.currentTimeMillis() - s);

    nearestLabels(p2Vec, inferer, text, 5);
    nearestLabels(p2Vec, inferer, text10000, 5);
  }

  private void nearestLabels(ParagraphVectors p2Vec, BatchInferer inferer, String text, int k)
  {
    INDArray v1 = p2Vec.inferVector(text);
    System.out.printf("%s (v1) = %s%n", text, p2Vec.nearestLabels(v1, k));

    INDArray v2 = inferer.inferVector(text);
    System.out.printf("%s (v2) = %s%n", text, p2Vec.nearestLabels(v2, k));
  }

  @Test
  public void test() throws Exception
  {
    ClassPathResource resource = new ClassPathResource("/raw_sentences.txt");
    SentenceIterator iter = new BasicLineIterator(resource.getFile());

    AbstractCache<VocabWord> cache = new AbstractCache<>();

    TokenizerFactory t = ModelConf.tokenizer();

    /**
     * if you don't have LabelAwareIterator handy, you can use synchronized labels generator
     * it will be used to label each document/sequence/line with its own label.
     *
     * But if you have LabelAwareIterator ready, you can provide it, for your in-house labels
     */
    LabelsSource source = new LabelsSource("DOC_");

    ParagraphVectors vec = new ParagraphVectors.Builder()
        .minWordFrequency(1)
        .iterations(5)
        .epochs(1)
        .layerSize(100)
        .learningRate(0.025)
        .labelsSource(source)
        .windowSize(5)
        .iterate(iter)
        .trainWordVectors(false)
        .vocabCache(cache)
        .tokenizerFactory(t)
        .sampling(0)
        .build();

    vec.fit();

    INDArray syn0 = vec.getLookupTable().getWeights();
    float[][] m = syn0.toFloatMatrix();
    float[] minmax = new float[] {Float.MAX_VALUE, Float.MIN_VALUE};
    for (float[] v : m) {
      v = VectorUtils.normalize(v);
      minmax[0] = Math.min(minmax[0], Floats.min(v));
      minmax[1] = Math.max(minmax[1], Floats.max(v));
    }
    System.out.println("minmax = " + Arrays.toString(minmax));
    /**
     * In training corpus we have few lines that contain pretty close words invloved.
     * These sentences should be pretty close to each other in vector space
     *
     * line 3721: This is my way .
     * line 6348: This is my case .
     * line 9836: This is my house .
     * line 12493: This is my world .
     * line 16393: This is my work .
     *
     * this is special sentence, that has nothing common with previous sentences
     * line 9853: We now have one .
     *
     * Note that docs are indexed from 0
     */
    double similarity1 = vec.similarity("DOC_9835", "DOC_12492");
    System.out.println("9836/12493 ('This is my house .'/'This is my world .') similarity: " + similarity1);

    double similarity2 = vec.similarity("DOC_3720", "DOC_16392");
    System.out.println("3721/16393 ('This is my way .'/'This is my work .') similarity: " + similarity2);

    double similarity3 = vec.similarity("DOC_6347", "DOC_3720");
    System.out.println("6348/3721 ('This is my case .'/'This is my way .') similarity: " + similarity3);

    // likelihood in this case should be significantly lower
    double similarityX = vec.similarity("DOC_3720", "DOC_9852");
    System.out.println("3721/9853 ('This is my way .'/'We now have one .') similarity: " + similarityX +
                       "(should be significantly lower)");

    // DOC_93124: We never made it
    // DOC_69123: We made it up
    // DOC_77660: We had it made
    System.out.println("[ParagraphVectorsTest/test] " + vec.nearestLabels("We made it", 3));

    File file = File.createTempFile("p2vec_", ".model");
    File file2 = File.createTempFile("p2vec_v2_", ".model");

    long p = System.currentTimeMillis();
    System.out.printf("v2 = %d%n", SequenceVectorsUtil.w2v_write(vec, file2).length());
    System.out.printf("saved v2.. %s, %d bytes, (%d msec)%n", file2, file2.length(), System.currentTimeMillis() - p);

    p = System.currentTimeMillis();
    ParagraphVectors loaded = SequenceVectorsUtil.p2v_load(file2);
    loaded.setTokenizerFactory(t);
    System.out.printf("loaded v2.. (%d msec)%n", System.currentTimeMillis() - p);
    System.out.println("We made it = " + loaded.nearestLabels("We made it", 3));

    System.out.println("p2vec writing...");
    p = System.currentTimeMillis();
    WordVectorSerializer.writeParagraphVectors(vec, file);
    System.out.printf("%s.length = %d (%d msec)%n", file, file.length(), System.currentTimeMillis() - p);

    /**
     * Let's assume that some time passed, and now we have new corpus to be used to weights update.
     * Instead of building new model over joint corpus, we can use weights update mode.
     */
    System.out.println("p2vec reading...");
    p = System.currentTimeMillis();
    ParagraphVectors model = WordVectorSerializer.readParagraphVectors(file);
    model.setTokenizerFactory(t);
    System.out.printf(".. (%d msec)%n", System.currentTimeMillis() - p);

    System.out.println("[ParagraphVectorsTest/test] " + model.nearestLabels("We made it", 3));
  }
}