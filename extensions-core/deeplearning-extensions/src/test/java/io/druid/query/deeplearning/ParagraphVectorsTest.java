package io.druid.query.deeplearning;

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

import java.io.File;

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