package io.druid.segment.lucene;

import io.druid.java.util.common.logger.Logger;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;

public class FSTBuilder
{
  public static final Logger LOG = new Logger(FSTBuilder.class);

  private static final int CHECK_INTERVAL = 10000;

  private final float reduction;
  private final Builder<Long> _builder = new Builder<>(FST.INPUT_TYPE.BYTE4, PositiveIntOutputs.getSingleton());
  private final IntsRefBuilder _scratch = new IntsRefBuilder();

  private boolean disabled;

  public FSTBuilder(float reduction)
  {
    this.reduction = reduction;
  }

  public void addEntry(String key, long ix) throws IOException
  {
    if (disabled) {
      return;
    }
    if (ix > 0 && ix % CHECK_INTERVAL == 0 && _builder.getNodeCount() > ix * reduction) {
      disabled = true;
      return;
    }
    _builder.add(Util.toUTF16(key, _scratch), ix);
  }

  public FST done(int cardinality) throws IOException
  {
    return disabled || _builder.getNodeCount() > cardinality * reduction ? null : _builder.finish();
  }

  public long getNodeCount()
  {
    return _builder.getNodeCount();
  }

  public long getArcCount()
  {
    return _builder.getArcCount();
  }
}
