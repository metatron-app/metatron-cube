package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.IngestionMode;
import io.druid.segment.indexing.DataSchema;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class HynixIndexTask extends HadoopIndexTask
{
  public static final String HYNIX_DATASOURCE = "hynix.index.original.datasource";

  private static HadoopIngestionSpec rewrite(HadoopIngestionSpec spec)
  {
    Map<String, Object> pathSpec = spec.getIOConfig().getPathSpec();
    if ("hynix".equals(pathSpec.get("type"))) {
      List<String> dataSources = Lists.newArrayList();
      for (Map elementSpec : (List<Map>) pathSpec.get("elements")) {
        String dataSourceName = Objects.toString(elementSpec.get("dataSource"), null);
        if (dataSourceName == null || dataSourceName.indexOf(';') >= 0) {
          throw new IllegalArgumentException("Datasource name should not be empty or contain ';'");
        }
        dataSources.add(dataSourceName);
      }
      String dataSourceList = StringUtils.join(dataSources, ';');

      // simple validation
      HadoopTuningConfig tuningConfig = spec.getTuningConfig();
      if (tuningConfig.getIngestionMode() != IngestionMode.REDUCE_MERGE) {
        throw new IllegalArgumentException("hynix type input spec only can be used with REDUCE_MERGE mode");
      }
      // keep this to be used as job name
      DataSchema dataSchema = spec.getDataSchema();
      HadoopIngestionSpec ingestionSpec = spec.withDataSchema(dataSchema.withDataSource(dataSourceList));
      if (!tuningConfig.getJobProperties().containsKey(HYNIX_DATASOURCE)) {
        Map<String, String> jobProperties = Maps.<String, String>newHashMap(tuningConfig.getJobProperties());
        jobProperties.put(HYNIX_DATASOURCE, dataSchema.getDataSource());
        ingestionSpec = ingestionSpec.withTuningConfig(tuningConfig.withJobProperty(jobProperties));
      }

      return ingestionSpec;
    }
    return spec;
  }

  public HynixIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? createNewId("index_hynix", spec) : id,
        rewrite(spec),
        hadoopCoordinates,
        hadoopDependencyCoordinates,
        classpathPrefix,
        jsonMapper,
        context
    );
  }
}
