package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.indexer.HadoopIngestionSpec;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class HynixIndexTask extends HadoopIndexTask
{
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
      return spec.withDataSchema(spec.getDataSchema().withDataSource(StringUtils.join(dataSources, ';')));
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
    super(id, rewrite(spec), hadoopCoordinates, hadoopDependencyCoordinates, classpathPrefix, jsonMapper, context);
  }
}
