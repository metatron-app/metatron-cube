package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;

import java.util.Arrays;
import java.util.Map;

public class SequenceCountingProcessor extends PostProcessingOperator.Abstract implements PostProcessingOperator.Local
{
  public static SequenceCountingProcessor INSTANCE = new SequenceCountingProcessor();

  @Override
  public QueryRunner<Row> postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query query, Map responseContext)
      {
        final Object accumulated = baseRunner.run(query, responseContext)
                                             .accumulate(0, new Accumulator<Integer, Object>()
                                             {
                                               @Override
                                               public Integer accumulate(Integer accumulated, Object in)
                                               {
                                                 return accumulated + 1;
                                               }
                                             });
        return Sequences.simple(
            Arrays.<Row>asList(new MapBasedRow(0, ImmutableMap.of("cardinality", accumulated)))
        );
      }
    };
  }
}
