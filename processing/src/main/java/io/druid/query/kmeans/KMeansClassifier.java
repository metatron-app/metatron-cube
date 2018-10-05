package io.druid.query.kmeans;

import com.google.common.base.Function;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Rows;
import io.druid.query.Classifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KMeansClassifier implements Classifier
{
  private final List<String> metrics;
  private final Centroid[] centroids;
  private final DistanceMeasure measure;
  private final double[] point;
  private final String tagColumn;

  public KMeansClassifier(List<String> metrics, Centroid[] centroids, DistanceMeasure measure, String tagColumn)
  {
    this.metrics = metrics;
    this.centroids = centroids;
    this.measure = measure;
    this.tagColumn = tagColumn;
    this.point = new double[metrics.size()];
  }

  @Override
  public Function<Object[], Object[]> init(List<String> outputColumns)
  {
    final int[] indices = GuavaUtils.indexOf(outputColumns, metrics);
    return new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(Object[] input)
      {
        for (int i = 0; i < indices.length; i++) {
          point[i] = Rows.parseDouble(input[indices[i]]);
        }
        Object[] output = Arrays.copyOf(input, input.length + 1);
        output[input.length] = measure.findNearest(centroids, point);
        return output;
      }
    };
  }

  @Override
  public Map<String, Object> apply(Map<String, Object> input)
  {
    for (int i = 0; i < metrics.size(); i++) {
      point[i] = Rows.parseDouble(input.get(metrics.get(i)));
    }
    Map<String, Object> updatable = MapBasedRow.toUpdatable(input);
    updatable.put(tagColumn, measure.findNearest(centroids, point));
    return updatable;
  }
}
