package io.druid.query;

import com.google.common.base.Function;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricBuilder<T> implements Function<Query<T>, ServiceMetricEvent.Builder>
{
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final ServiceEmitter emitter;
  private final AtomicLong accumulator;

  public CPUTimeMetricBuilder(
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      ServiceEmitter emitter
  )
  {
    this.builderFn = builderFn;
    this.emitter = emitter;
    this.accumulator = new AtomicLong();
  }

  @Override
  public ServiceMetricEvent.Builder apply(Query<T> input)
  {
    return builderFn.apply(input);
  }

  public QueryRunner<T> accumulate(QueryRunner<T> runner)
  {
    return CPUTimeMetricQueryRunner.safeBuild(runner, builderFn, emitter, accumulator, false);
  }

  public QueryRunner<T> report(QueryRunner<T> runner)
  {
    return CPUTimeMetricQueryRunner.safeBuild(runner, builderFn, emitter, accumulator, true);
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public AtomicLong getAccumulator()
  {
    return accumulator;
  }
}
