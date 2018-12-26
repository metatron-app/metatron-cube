package io.druid.query.egads;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.models.adm.AnomalyDetectionModel;
import com.yahoo.egads.models.tsmm.TimeSeriesModel;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
@JsonTypeName("anomaly")
public class AnomalyPostProcessor
    extends PostProcessingOperator.Abstract
    implements PostProcessingOperator.SchemaResolving
{
  private final String timestampColumn;
  private final String metricColumn;
  private final String predictColumn;
  private final String anomalyColumn;
  private final String tsModel;
  private final String adModel;
  private final Map<String, Object> parameters;

  private final Granularity timeGranularity;
  private final String tsModelColumn;

  @JsonCreator
  public AnomalyPostProcessor(
      @JsonProperty("timestampColumn") String timestampColumn,
      @JsonProperty("metricColumn") String metricColumn,
      @JsonProperty("predictColumn") String predictColumn,
      @JsonProperty("anomalyColumn") String anomalyColumn,
      @JsonProperty("tsModel") String tsModel,
      @JsonProperty("adModel") String adModel,
      @JsonProperty("parameters") Map<String, Object> parameters,
      @JsonProperty("timeGranularity") Granularity timeGranularity,
      @JsonProperty("tsModelColumn") String tsModelColumn
  )
  {
    this.timestampColumn = Preconditions.checkNotNull(timestampColumn, "'timestampColumn' cannot be null");
    this.metricColumn = Preconditions.checkNotNull(metricColumn, "'metricColumn' cannot be null");
    this.predictColumn = predictColumn;
    this.anomalyColumn = anomalyColumn;
    this.tsModel = Preconditions.checkNotNull(tsModel, "'tsModel' cannot be null");
    this.adModel = adModel;
    this.parameters = parameters == null ? ImmutableMap.<String, Object>of() : parameters;
    Preconditions.checkNotNull(Utils.getTS(tsModel), "invalid tsModel " + tsModel);
    if (adModel != null) {
      Preconditions.checkNotNull(Utils.getAD(adModel), "invalid adModel " + adModel);
      Preconditions.checkNotNull(anomalyColumn, "'anomalyColumn' cannot be null");
    }
    this.timeGranularity = timeGranularity;
    this.tsModelColumn = tsModelColumn;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        Properties properties = Utils.initProperties(parameters);
        Granularity granularity = Optional.fromNullable(timeGranularity).or(query.getGranularity());
        Preconditions.checkNotNull(granularity, "cannot extract 'timeGranularity'");

        final TsModel ts = Utils.getTS(tsModel);
        for (Parameter parameter : ts.parameters()) {
          if (parameter.isOptional() && parameter.getDefaultValue() != null) {
            properties.put(parameter.getName(), String.valueOf(parameter.getDefaultValue()));
          }
        }
        final TimeSeriesModel model = ts.newInstance(properties);
        final TimeSeries timeSeries = Utils.tableToTimeSeries(
            baseRunner.run(query, responseContext),
            timestampColumn,
            metricColumn,
            granularity
        );
        final TimeSeries.DataSequence observed = timeSeries.data;
        final TimeSeries.DataSequence expected = new TimeSeries.DataSequence();
        for (TimeSeries.Entry entry : observed) {
          expected.add(new TimeSeries.Entry(entry.time, 0));
        }

        try {
          model.reset();
          model.train(observed);
          model.predict(expected);

          if (predictColumn != null) {
            for (int i = 0; i < timeSeries.size(); i++) {
              RowEntry<Map<String, Object>> entry = (RowEntry<Map<String, Object>>) observed.get(i);
              entry.row.put(predictColumn, expected.get(i).value);
            }
          }
          if (tsModelColumn != null && timeSeries.size() > 0) {
            RowEntry<Map<String, Object>> entry = (RowEntry<Map<String, Object>>) observed.get(0);
            Map<String, Object> modelDesc = ImmutableMap.<String, Object>of(
                "name", model.getModelName(),
                "type", model.getModelType()
            );
            entry.row.put(tsModelColumn, modelDesc);
          }

          if (adModel != null) {
            final AdModel ad = Utils.getAD(adModel);
            for (Parameter parameter : ad.parameters()) {
              if (parameter.isOptional() && parameter.getDefaultValue() != null) {
                properties.put(parameter.getName(), String.valueOf(parameter.getDefaultValue()));
              }
            }
            final AnomalyDetectionModel detector = ad.newInstance(properties);
            detector.tune(observed, expected, null);

            for (Anomaly.Interval interval : detector.detect(observed, expected)) {
              RowEntry<Map<String, Object>> entry = (RowEntry<Map<String, Object>>) observed.get(interval.index);
              entry.row.put(anomalyColumn, true);   // todo
            }
          }
          return Sequences.simple(
              Iterables.transform(
                  observed, new Function<TimeSeries.Entry, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(TimeSeries.Entry input)
                    {
                      return ((RowEntry<Map<String, Object>>) input).row;
                    }
                  }
              )
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }

  @Override
  public IncrementalIndexSchema resolve(Query query, IncrementalIndexSchema input, ObjectMapper mapper)
  {
    if (predictColumn == null && tsModelColumn == null) {
      return input;
    }
    List<AggregatorFactory> metrics = Lists.newArrayList(Arrays.asList(input.getMetrics()));
    if (predictColumn != null) {
      metrics.add(new RelayAggregatorFactory(predictColumn, ValueDesc.FLOAT));
    }
    if (tsModelColumn != null) {
      metrics.add(new RelayAggregatorFactory(tsModelColumn, ValueDesc.MAP));
    }
    return input.withMetrics(metrics.toArray(new AggregatorFactory[0]));
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getMetricColumn()
  {
    return metricColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredictColumn()
  {
    return predictColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAnomalyColumn()
  {
    return anomalyColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTsModel()
  {
    return tsModel;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAdModel()
  {
    return adModel;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Object> getParameters()
  {
    return parameters;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Granularity getTimeGranularity()
  {
    return timeGranularity;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTsModelColumn()
  {
    return tsModelColumn;
  }
}
