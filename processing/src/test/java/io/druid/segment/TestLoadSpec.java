package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.Evaluation;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.Validation;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.List;
import java.util.Map;

public class TestLoadSpec extends IncrementalIndexSchema
{
  private final Map<String, Object> parser;
  private final List<String> columns;
  private final TimestampSpec timestampSpec;

  private final boolean enforceType;
  private final List<Evaluation> evaluations;
  private final List<Validation> validations;

  private final IndexSpec indexSpec;

  @JsonCreator
  public TestLoadSpec(
      @JsonProperty("minTimestamp") long minTimestamp,
      @JsonProperty("queryGran") Granularity queryGran,
      @JsonProperty("segmentGran") Granularity segmentGran,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] metricsSpec,
      @JsonProperty("evaluations") List<Evaluation> evaluations,
      @JsonProperty("validations") List<Validation> validations,
      @JsonProperty("enforceType") boolean enforceType,
      @JsonProperty("rollup") boolean rollup,
      @JsonProperty("fixedSchema") boolean fixedSchema,
      @JsonProperty("indexSpec") IndexSpec indexSpec
  )
  {
    super(minTimestamp, queryGran, segmentGran, dimensionsSpec, metricsSpec, rollup, fixedSchema);
    this.parser = parser;
    this.columns = columns;
    this.timestampSpec = timestampSpec;
    this.evaluations = evaluations == null ? ImmutableList.<Evaluation>of() : evaluations;
    this.validations = validations == null ? ImmutableList.<Validation>of() : validations;
    this.enforceType = enforceType;
    this.indexSpec = indexSpec;
  }

  public InputRowParser getParser(ObjectMapper mapper, boolean ignoreInvalidRows)
  {
    DimensionsSpec dimensionsSpec = getDimensionsSpec();
    ParseSpec spec;
    if (parser == null) {
      spec = new DelimitedParseSpec(timestampSpec, dimensionsSpec, null, null, columns);
    } else {
      if (!parser.containsKey("columns")) {
        parser.put("columns", columns);
      }
      spec = mapper.convertValue(parser, ParseSpec.class);
      spec = spec.withDimensionsSpec(dimensionsSpec).withTimestampSpec(timestampSpec);
    }
    return InputRowParsers.wrap(
        new StringInputRowParser(spec, null),
        getMetrics(),
        evaluations,
        validations,
        enforceType,
        ignoreInvalidRows
    );
  }

  public IndexSpec getIndexingSpec()
  {
    return indexSpec == null ? new IndexSpec() : indexSpec;
  }
}
