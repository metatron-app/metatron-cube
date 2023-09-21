/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.CardinalityMeta;
import io.druid.query.CardinalityMetaQuery;
import io.druid.query.Query;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.SetAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQuery.Builder;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.timeline.DataSegment;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class DruidTable implements TranslatableTable, BuiltInMetadata.MaxRowCount.Handler
{
  private static final Logger LOG = new Logger(DruidTable.class);

  private final TableDataSource dataSource;
  private final QuerySegmentWalker segmentWalker;
  protected final String tenantColumn;

  private RowSignature signature;
  private int numSegments = -1;
  private Statistic statistic;
  private Map<String, Map<String, String>> descriptors;

  private Map<String, long[]> cardinalityRanges;
  private final Map<List<String>, double[]> cardinalityCoeffs = Maps.newHashMap();
  private final Map<String, Double> selectivity = Maps.newHashMap();
  private final Map<List<String>, Double> cardinalities = Maps.newHashMap();

  private Set<String> tenants;

  public DruidTable(
      TableDataSource dataSource,
      QuerySegmentWalker segmentWalker,
      RowSignature signature,
      String tenantColumn
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.segmentWalker = Preconditions.checkNotNull(segmentWalker, "segmentWalker");
    this.signature = signature;
    this.tenantColumn = tenantColumn;
  }

  public boolean isMultiTenent()
  {
    return tenantColumn != null;
  }

  public void update(DataSegment segment, boolean added)
  {
    Supplier<Holder> update = Suppliers.memoize(
        () -> build(segment.getDataSource(), new SpecificSegmentSpec(segment.toDescriptor()), segmentWalker)
    );
    if (signature != null && added) {
      signature = RowSignature.from(signature.merge(update.get().signature));
    }

    if (numSegments >= 0) {
      numSegments += added ? 1 : -1;
    }

    if (descriptors != null) {
      Map<String, Map<String, String>> updated = update.get().descriptors;
      for (Map.Entry<String, Map<String, String>> entry : descriptors.entrySet()) {
        Map<String, String> value = updated.remove(entry.getKey());
        if (value != null && !value.equals(entry.getValue())) {
          if (value.size() > entry.getValue().size()) {
            entry.setValue(value);
          } else {
            LOG.info("conflicts on %s (%s vs %s)", entry.getKey(), entry.getValue(), value);
          }
        }
      }
      descriptors.putAll(updated);
    }

    if (statistic != null) {
      final long rowCount = update.get().rowCount;
      final long delta = added ? rowCount : -rowCount;
      statistic = Statistics.of(statistic.getRowCount() + delta, ImmutableList.of());
    }
    if (tenantColumn != null) {
      final List<String> newcomers = getTenants(QuerySegmentSpecs.create(segment.toDescriptor()), tenantColumn);
      if (tenants == null) {
        tenants = Sets.newHashSet(newcomers);
      } else {
        tenants.addAll(newcomers);
      }
    }
    selectivity.clear();
    cardinalities.clear();
  }

  public String getName()
  {
    return dataSource.getName();
  }

  public TableDataSource getDataSource()
  {
    return dataSource;
  }

  public synchronized RowSignature getRowSignature()
  {
    if (signature == null) {
      updateAll();
    }
    return signature;
  }

  public synchronized Map<String, Map<String, String>> getDescriptors()
  {
    if (descriptors == null) {
      updateAll();
    }
    return descriptors == null ? ImmutableMap.of() : descriptors;
  }

  @Override
  public synchronized Statistic getStatistic()
  {
    if (statistic == null) {
      if (signature == null) {
        updateAll();
      } else {
        updateStats();
      }
    }
    return statistic == null ? Statistics.of(-1, ImmutableList.of()) : statistic;
  }

  public synchronized long[] cardinalityRange(String dimension)
  {
    if (cardinalityRanges == null) {
      updateAll();
    }
    return cardinalityRanges == null ? null : cardinalityRanges.get(dimension);
  }

  public synchronized int numSegments()
  {
    if (numSegments < 0) {
      updateAll();
    }
    return numSegments;
  }

  public Double estimateCardinality(
      TableScan scan,
      RelMetadataQuery mq,
      List<String> groupKey,
      RexNode predicate,
      QueryMaker context
  )
  {
    Double estimate = Double.NaN;
    if (numSegments() > 1) {
      estimate = CardinalityMeta.estimate(
          cardinalityCoeffs.computeIfAbsent(groupKey, this::coeff),
          () -> mq.getRowCount(scan) * mq.getSelectivity(scan, predicate)
      );
    }
    if (!Double.isFinite(estimate) && groupKey.size() == 1) {
      long[] range = cardinalityRanges.get(groupKey.get(0));
      if (range != null && range[0] << 2 > range[1]) {
        return Double.valueOf(range[1]) * mq.getSelectivity(scan, predicate);    // todo: min ? max ?
      }
    }
    if (!Double.isFinite(estimate)) {
      List<String> key = concat(groupKey, predicate);
      estimate = cardinalities.computeIfAbsent(key, k -> cardinality(scan, groupKey, predicate, context));
    }
    return estimate;
  }

  private double[] coeff(List<String> groupKey)
  {
    Query<CardinalityMeta> query = CardinalityMetaQuery.of(dataSource, groupKey);
    CardinalityMeta meta = Sequences.only(QueryRunners.run(query, segmentWalker), null);
    return meta == null || meta.getParams() == null ? null : meta.getParams()[0];
  }

  private static Double cardinality(TableScan rel, List<String> groupKey, RexNode predicate, QueryMaker context)
  {
    if (context == null || !context.getPlannerConfig().isUseEstimationQuery()) {
      return null;
    }
    RowSignature signature = RowSignature.from(rel.getRowType());
    Filtration filtration = toFiltration(rel, predicate, context, signature);
    if (filtration == null) {
      return null;
    }
    String table = Utils.tableName(rel.getTable());
    if (groupKey.contains(Row.TIME_COLUMN_NAME)) {
      return null;    // todo ??
    }
    return (double) Utils.estimateCardinality(table, filtration, groupKey, context);
  }

  @Nullable
  private static Filtration toFiltration(RelNode rel, RexNode predicate, QueryMaker context, RowSignature signature)
  {
    if (predicate == null) {
      return Filtration.create(null);
    }
    RexBuilder builder = rel.getCluster().getRexBuilder();
    DimFilter filter = Expressions.toFilter(context.getPlannerContext(), signature, builder, predicate);
    return filter == null ? null : Filtration.create(filter).optimize(signature);
  }

  private static List<String> concat(List<String> groupKey, RexNode predicate)
  {
    return predicate == null ? groupKey : GuavaUtils.concat(groupKey, predicate.toString());
  }

  public Double estimateSelectivity(TableScan scan, RexNode predicate, QueryMaker context)
  {
    return selectivity.computeIfAbsent(predicate.toString(), k -> selectivity(scan, predicate, context));
  }

  private static Double selectivity(TableScan rel, RexNode predicate, QueryMaker context)
  {
    if (context == null || !context.getPlannerConfig().isUseEstimationQuery()) {
      return null;
    }
    Filtration filtration = toFiltration(rel, predicate, context, RowSignature.from(rel.getRowType()));
    if (filtration == null) {
      return null;
    }
    long[] estimation = Utils.estimateSelectivity(Utils.tableName(rel.getTable()), filtration, context);
    double selectivity = estimation[1] == 0 ? 0D : estimation[0] / (double) estimation[1];
    return selectivity;
  }

  private void updateAll()
  {
    long start = System.currentTimeMillis();
    Holder update = build(dataSource.getName(), QuerySegmentSpec.ETERNITY, segmentWalker);
    this.signature = update.signature;
    this.numSegments = update.numSegments;
    this.cardinalityRanges = update.cardinalities;
    this.descriptors = update.descriptors;
    this.statistic = Statistics.of(update.rowCount, ImmutableList.of());
    LOG.info("Refreshed schema of [%s].. %,d msec", dataSource.getName(), System.currentTimeMillis() - start);
  }

  private void updateStats()
  {
    long start = System.currentTimeMillis();
    TimeseriesQuery query = TimeseriesQuery.countAll(dataSource);
    Row result = Sequences.only(QueryRunners.run(query.withRandomId(), segmentWalker), null);
    Long count = result == null ? null : result.getLong("count");
    if (count != null) {
      statistic = Statistics.of(count, ImmutableList.of());
      LOG.info("Refreshed stats of [%s].. %,d msec", dataSource.getName(), System.currentTimeMillis() - start);
    }
  }

  public Set<String> getTenants()
  {
    if (tenantColumn == null) {
      return ImmutableSet.of();
    }
    synchronized (tenantColumn) {
      if (tenants == null) {
        tenants = Sets.newHashSet(getTenants(QuerySegmentSpec.ETERNITY, tenantColumn));
      }
    }
    return tenants;
  }

  @Override
  public Double getMaxRowCount(RelNode r, RelMetadataQuery mq)
  {
    return getStatistic().getRowCount();
  }

  private static class Holder
  {
    private final RowSignature signature;
    private final Map<String, long[]> cardinalities;
    private final Map<String, Map<String, String>> descriptors;
    private final int numSegments;
    private final long rowCount;

    private Holder(
        RowSignature signature,
        int numSegments,
        Map<String, long[]> cardinalities,
        Map<String, Map<String, String>> descriptors,
        long rowCount
    )
    {
      this.signature = signature;
      this.numSegments = numSegments;
      this.cardinalities = cardinalities;
      this.descriptors = descriptors;
      this.rowCount = rowCount;
    }
  }

  private static Holder build(String datasource, QuerySegmentSpec segmentSpec, QuerySegmentWalker segmentWalker)
  {
    SegmentMetadataQuery query = SegmentMetadataQuery.schema(datasource, segmentSpec);
    long s = System.currentTimeMillis();
    SegmentAnalysis segment = Sequences.only(QueryRunners.run(query.withRandomId(), segmentWalker));
    LOG.info("analysis [%s] took %,d msec", datasource, (System.currentTimeMillis() - s));
    Map<String, long[]> cardinalities = Maps.newHashMap();
    Map<String, Map<String, String>> descriptors = Maps.newHashMap();
    for (Map.Entry<String, ColumnAnalysis> entry : segment.getColumns().entrySet()) {
      String column = entry.getKey();
      ColumnAnalysis analysis = entry.getValue();
      descriptors.put(column, analysis.getDescriptor());
      cardinalities.put(column, analysis.getCardinality());
    }
    int numSegments = segment.getNumSegments();
    RowSignature signature = RowSignature.builderFrom(segment.asSignature().explodeNested()).sort().build();

    return new Holder(signature, numSegments, cardinalities, descriptors, segment.getNumRows());
  }

  @SuppressWarnings("unchecked")
  private List<String> getTenants(QuerySegmentSpec spec, String tenantColumn)
  {
    ValueDesc resolved = getRowSignature().resolve(tenantColumn, ValueDesc.UNKNOWN);
    AggregatorFactory aggr = new SetAggregatorFactory(
        "_tenants",
        tenantColumn,
        resolved.unwrapDimension(),
        65536,
        false
    );
    TimeseriesQuery ts = new Builder().dataSource(dataSource).intervals(spec).aggregators(aggr).build();
    Row v = Sequences.only(QueryRunners.run(ts.withId(UUID.randomUUID().toString()), segmentWalker), null);
    if (v != null && v.get("_tenants") != null) {
      return (List<String>) v.get("_tenants");
    }
    return ImmutableList.of();
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.TABLE;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return getRowSignature().toRelDataType(typeFactory);
  }

  @Override
  public boolean isRolledUp(String column)
  {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      final String column,
      final SqlCall call,
      final SqlNode parent,
      final CalciteConnectionConfig config
  )
  {
    return true;
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table, Arrays.asList());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return Objects.equals(dataSource, ((DruidTable) o).dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(dataSource);
  }

  public static final class Tenant extends DruidTable
  {
    private final DruidTable source;
    private final String tenant;

    public Tenant(DruidTable source, String tenant, QuerySegmentWalker segmentWalker, RowSignature signature)
    {
      super(source.getDataSource(), segmentWalker, signature, null);
      this.source = Preconditions.checkNotNull(source, "'source' should not be null");
      this.tenant = Preconditions.checkNotNull(tenant, "'tenant' should not be null");
      Preconditions.checkArgument(
          source.isMultiTenent(), "[%s] should be a multi-tenant table", source.dataSource.getName()
      );
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
    {
      RelNode scan = source.toRel(context, table);
      int index = scan.getRowType().getFieldNames().indexOf(source.tenantColumn);
      if (index < 0) {
        return new LogicalValues(scan.getCluster(), scan.getTraitSet(), scan.getRowType(), ImmutableList.of());
      }
      RelDataType type = scan.getRowType().getFieldList().get(index).getType();
      RexBuilder builder = context.getCluster().getRexBuilder();
      RexInputRef column = builder.makeInputRef(type, index);
      RexLiteral literal = builder.makeLiteral(tenant);

      RexNode condition = builder.makeCall(SqlStdOperatorTable.EQUALS, Arrays.asList(column, literal));
      return LogicalFilter.create(scan, condition);
    }

    @Override
    public RowSignature getRowSignature()
    {
      return source.getRowSignature();
    }

    @Override
    public Map<String, Map<String, String>> getDescriptors()
    {
      return source.getDescriptors();
    }

    @Override
    public Statistic getStatistic()
    {
      return source.getStatistic();
    }

    @Override
    public void update(DataSegment segment, boolean added)
    {
      // should not be happened
    }

    @Override
    public Schema.TableType getJdbcTableType()
    {
      return Schema.TableType.VIEW;
    }
  }

  private static class CardinalityEntry
  {

  }
}
