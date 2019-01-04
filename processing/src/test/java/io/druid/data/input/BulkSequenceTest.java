package io.druid.data.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import io.druid.common.Yielders;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BulkSequenceTest
{
  @Test
  public void test() throws IOException
  {
    Sequence<Row> rows = Sequences.simple(Arrays.<Row>asList(
        cr(0), cr(1), cr(2), cr(3), cr(4)
    ));
    BulkSequence bulk = new BulkSequence(rows, Arrays.asList(ValueDesc.LONG, ValueDesc.STRING), 2);

    final List<long[]> longs = Sequences.toList(Sequences.map(
        bulk, new Function<Row, long[]>()
        {
          @Override
          public long[] apply(Row input)
          {
            return (long[]) ((BulkRow) input).getValues()[0];
          }
        }
    ));
    Assert.assertArrayEquals(new long[]{0, 1}, longs.get(0));
    Assert.assertArrayEquals(new long[]{2, 3}, longs.get(1));
    Assert.assertArrayEquals(new long[]{4}, longs.get(2));

    final List<String[]> strings = Sequences.toList(Sequences.map(
        bulk, new Function<Row, String[]>()
        {
          @Override
          public String[] apply(Row input)
          {
            return (String[]) ((BulkRow) input).getValues()[1];
          }
        }
    ));
    Assert.assertArrayEquals(new String[]{"0", "1"}, strings.get(0));
    Assert.assertArrayEquals(new String[]{"2", "3"}, strings.get(1));
    Assert.assertArrayEquals(new String[]{"4"}, strings.get(2));

    Yielder<Row> yielder = bulk.toYielder(null, new Yielders.Yielding<Row>());
    Assert.assertArrayEquals(new long[]{0, 1}, (long[]) ((BulkRow) yielder.get()).getValues()[0]);
    yielder = yielder.next(null);
    Assert.assertArrayEquals(new long[]{2, 3}, (long[]) ((BulkRow) yielder.get()).getValues()[0]);
    yielder = yielder.next(null);
    Assert.assertArrayEquals(new long[]{4}, (long[]) ((BulkRow) yielder.get()).getValues()[0]);
    Assert.assertTrue(yielder.isDone());

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String s = mapper.writeValueAsString(bulk);
    List<Row> deserialized = mapper.readValue(s, new TypeReference<List<Row>>() {});
    Assert.assertEquals(Arrays.asList(0, 1), ((BulkRow) deserialized.get(0)).getValues()[0]);
    Assert.assertEquals(Arrays.asList("0", "1"), ((BulkRow) deserialized.get(0)).getValues()[1]);
    Assert.assertEquals(Arrays.asList(2, 3), ((BulkRow) deserialized.get(1)).getValues()[0]);
    Assert.assertEquals(Arrays.asList("2", "3"), ((BulkRow) deserialized.get(1)).getValues()[1]);
    Assert.assertEquals(Arrays.asList(4), ((BulkRow) deserialized.get(2)).getValues()[0]);
    Assert.assertEquals(Arrays.asList("4"), ((BulkRow) deserialized.get(2)).getValues()[1]);
  }

  private CompactRow cr(int x)
  {
    return new CompactRow(new Object[]{x, String.valueOf(x)});
  }
}