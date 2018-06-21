package io.druid.query.egads;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.yahoo.egads.data.TimeSeries;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.data.Rows;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class Utils
{
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className, Class<T> clazz, Properties properties)
  {
    try {
      Class<?> tsModelClass = Class.forName(className);
      Constructor<?> constructor = tsModelClass.getConstructor(Properties.class);
      return clazz.cast(constructor.newInstance(properties));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static final int TO_SECONDS = 1000;

  public static TimeSeries tableToTimeSeries(
      final Sequence<Map<String, Object>> rows,
      final String timeColumn,
      final String metricColumn,
      final Granularity granularity
  )
  {
    final List<TimeSeries.Entry> entries = Lists.newArrayList();
    rows.accumulate(
        null, new Accumulator<Object, Map<String,Object>>()
        {
          private DateTime nextTime;
          private float prevValue;

          @Override
          public Object accumulate(Object accumulated, Map<String, Object> input)
          {
            final DateTime current = granularity.bucketStart(DateTimes.utc(Rows.parseLong(input.get(timeColumn))));
            final float value = Rows.parseFloat(input.get(metricColumn));
            if (nextTime == null || nextTime.equals(current)) {
              nextTime = granularity.bucketEnd(current);
              prevValue = value;
            } else if (nextTime.isBefore(current)) {
              final Interval interval = Intervals.of(nextTime, current);
              final int count = Iterables.size(granularity.getStartIterable(interval));
              final float delta = (value - prevValue) / count;
              int i = 1;
              for (DateTime start : granularity.getStartIterable(interval)) {
                float dummyValue = prevValue + delta * i++;
                Map<String, Object> dummyRow = Maps.newHashMap();
                dummyRow.put(timeColumn, start);
                dummyRow.put(metricColumn, dummyValue);
                entries.add(new RowEntry<Map<String, Object>>(start.getMillis() / TO_SECONDS, dummyValue, dummyRow));
              }
            } else {
              throw new IllegalStateException("invalid granularity or not sorted on time");  // todo
            }
            entries.add(new RowEntry<Map<String, Object>>(current.getMillis() / TO_SECONDS, value, input));
            return null;
          }
        }
    );
    return new RowTimeSeries(entries);
  }

  static Map<String, TsModel> TS_MODELS = Maps.newHashMap();
  static Map<String, AdModel> AD_MODELS = Maps.newHashMap();

  static {
    for (TsModel model : TsModel.values()) {
      TS_MODELS.put(model.name(), model);
      TS_MODELS.put(model.name().toLowerCase(), model);
    }
    for (AdModel model : AdModel.values()) {
      AD_MODELS.put(model.name(), model);
      AD_MODELS.put(model.name().toLowerCase(), model);
    }
  }

  static TsModel getTS(String tsModel)
  {
    TsModel model = TS_MODELS.get(tsModel);
    if (tsModel != null && model == null) {
      model = TS_MODELS.get(tsModel.toLowerCase());
    }
    return model;
  }

  static AdModel getAD(String adModel)
  {
    AdModel model = AD_MODELS.get(adModel);
    if (adModel != null && model == null) {
      model = AD_MODELS.get(adModel.toLowerCase());
    }
    return model;
  }

  static Granularity getGranularity(Granularity granularity, TimeSeries ts)
  {
    if (granularity == null && ts.size() > 1) {
      int period = Ints.checkedCast(ts.data.get(1).time - ts.data.get(0).time);
      return toPeriodGranularity(period);
    }
    return granularity;
  }

  static PeriodGranularity toPeriodGranularity(long period)
  {
    return new PeriodGranularity(Period.millis(Ints.checkedCast(period)), null, null);
  }

  static Properties initProperties(Map<String, Object> parameters)
  {
    Properties properties = new Properties();
    properties.put("OUTPUT", "");   // NPE in AdaptiveKernelDensityChangePointDetector
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      final Object value = entry.getValue();
      if (value != null) {
        properties.put(entry.getKey(), toString(value));
      }
    }
    return properties;
  }

  private static String toString(Object value)
  {
    String string;
    if (value == null) {
      string = null;
    } else if (value.getClass().isArray()) {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < Array.getLength(value); i++) {
        if (b.length() > 0) {
          b.append(',');
        }
        b.append(String.valueOf(Array.get(value, i)));
      }
      string = b.toString();
    } else if (value instanceof Map) {
      StringBuilder b = new StringBuilder();
      for (Map.Entry entry : ((Map<?, ?>)value).entrySet()) {
        if (b.length() > 0) {
          b.append(',');
        }
        b.append(toString(entry.getKey())).append('#').append(toString(entry.getValue()));
      }
      string = b.toString();
    } else {
      string = String.valueOf(value);
    }
    return string;
  }
}
