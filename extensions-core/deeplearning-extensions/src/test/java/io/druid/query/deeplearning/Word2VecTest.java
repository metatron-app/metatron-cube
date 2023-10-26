package io.druid.query.deeplearning;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.junit.Test;
import org.nd4j.common.io.ClassPathResource;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

/**
 * This is simple example for model weights update after initial vocab building.
 * If you have built your w2v model, and some time later you've decided that it can be
 * additionally trained over new corpus, here's an example how to do it.
 * <p>
 * PLEASE NOTE: At this moment, no new words will be added to vocabulary/model.
 * Only weights update process will be issued. It's often called "frozen vocab training".
 *
 * @author raver119@gmail.com
 */
public class Word2VecTest
{
  @Test
  public void word2vec() throws Exception
  {
    String filePath = new ClassPathResource("raw_sentences.txt").getFile().getAbsolutePath();

    System.out.println("Load & Vectorize Sentences....");
    // Strip white space before and after for each line
    SentenceIterator iter = new BasicLineIterator(filePath);
    // Split on white spaces in the line to get words

    System.out.println("Building model....");
    Word2Vec word2Vec1 = new Word2Vec.Builder()
        .minWordFrequency(5)
        .iterations(1)
        .epochs(1)
        .layerSize(100)
        .seed(42)
        .windowSize(5)
        .iterate(iter)
        .tokenizerFactory(ModelConf.tokenizer())
        .build();

    System.out.println("Fitting Word2Vec model....");
    word2Vec1.fit();

    double[] vector1 = word2Vec1.getWordVector("day");
    System.out.println("day = " + Arrays.toString(Arrays.copyOf(vector1, 3)));

    Collection<String> lst = word2Vec1.wordsNearest("day", 10);
    System.out.println("Closest words to 'day' on 1st run: " + lst);

    File file = File.createTempFile("word2vec_", ".model");
    File file2 = File.createTempFile("word2vec_v2_", ".model");

    long p = System.currentTimeMillis();
    SequenceVectorsUtil.w2v_write(word2Vec1, file2);
    System.out.printf("saved v2.. %d bytes, (%d msec)%n", file2.length(), System.currentTimeMillis() - p);

    p = System.currentTimeMillis();
    Word2Vec loaded = SequenceVectorsUtil.w2v_load(file2);
    System.out.printf("loaded v2.. (%d msec)%n", System.currentTimeMillis() - p);

    double[] x = loaded.getWordVector("day");
    System.out.println("day = " + Arrays.toString(Arrays.copyOf(x, 3)));
    lst = loaded.wordsNearest("day", 10);
    System.out.println("Closest words to 'day' on 2nd run: " + lst);

    /**
     * at this moment we're supposed to have model built, and it can be saved for future use.
     */
    p = System.currentTimeMillis();
    WordVectorSerializer.writeWord2VecModel(word2Vec1, file);
    System.out.printf("%s.length = %d(%d msec)%n", file, file.length(), System.currentTimeMillis() - p);

    /**
     * Let's assume that some time passed, and now we have new corpus to be used to weights update.
     * Instead of building new model over joint corpus, we can use weights update mode.
     */
    p = System.currentTimeMillis();
    System.out.println("Word2vec reading...");
    Word2Vec word2Vec2 = WordVectorSerializer.readWord2VecModel(file);
    System.out.printf("loaded.. (%d msec)%n", System.currentTimeMillis() - p);

    double[] vector2 = word2Vec2.getWordVector("day");
    System.out.println("day = " + Arrays.toString(Arrays.copyOf(vector2, 3)));

    lst = word2Vec2.wordsNearest("day", 10);
    System.out.println("Closest words to 'day' on 2nd run: " + lst);

    /**
     * PLEASE NOTE: after model is restored, it's still required to set SentenceIterator and TokenizerFactory, if you're going to train this model
     */
    SentenceIterator iterator = new BasicLineIterator(filePath);
    TokenizerFactory tokenizerFactory = new DefaultTokenizerFactory();
    tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor());

    word2Vec2.setTokenizerFactory(tokenizerFactory);
    word2Vec2.setSentenceIterator(iterator);


    System.out.println("Word2vec uptraining...");

    word2Vec2.fit();

    double[] vector3 = word2Vec2.getWordVector("day");
    System.out.println("day = " + Arrays.toString(Arrays.copyOf(vector3, 3)));

    lst = word2Vec2.wordsNearest("day", 10);
    System.out.println("Closest words to 'day' on 3rd run: " + lst);
  }
}
