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

package io.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.type.RelDataType;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYAreaRenderer;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public interface ChartFormatters
{
  @JsonTypeName("pie")
  class Pie implements ResultFormat
  {
    private final String title;
    private final int width;
    private final int height;

    @JsonCreator
    public Pie(
        @JsonProperty("title") String title,
        @JsonProperty("width") Integer width,
        @JsonProperty("height") Integer height
    )
    {
      this.title = title;
      this.width = width == null ? 500 : width;
      this.height = height == null ? 500 : height;
    }

    @Override
    public String contentType()
    {
      return "image/png";
    }

    @Override
    public Writer createFormatter(
        final OutputStream os,
        final ObjectMapper mapper,
        final RelDataType rowType
    ) throws IOException
    {
      final List<String> columnNames = RowSignature.columnNames(rowType);
      return new Writer()
      {
        private final DefaultPieDataset dataset = new DefaultPieDataset();

        @Override
        public void writeRow(Object[] row) throws IOException
        {
          if (columnNames.size() == 2 && row[1] instanceof Number) {
            dataset.setValue(Objects.toString(row[0], null), (Number) row[1]);
          } else {
            for (int i = 0; i < row.length; i++) {
              if (row[i] instanceof Number) {
                dataset.setValue(columnNames.get(i), (Number) row[i]);
              }
            }
          }
        }

        @Override
        public void end() throws IOException
        {
          JFreeChart chart = ChartFactory.createPieChart(title, dataset, true, false, false);
          PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
              "{0}: {1} ({2})", NumberFormat.getInstance(), NumberFormat.getPercentInstance());
          PiePlot plot = (PiePlot) chart.getPlot();
          plot.setSimpleLabels(true);
          plot.setLabelGenerator(gen);

          ChartUtils.writeChartAsPNG(os, chart, width, height);
        }
      };
    }
  }

  @JsonTypeName("XYLine")
  class XYLine implements ResultFormat
  {
    private final String title;
    private final int width;
    private final int height;
    private final String X;
    private final String Y;

    @JsonCreator
    public XYLine(
        @JsonProperty("title") String title,
        @JsonProperty("width") Integer width,
        @JsonProperty("height") Integer height,
        @JsonProperty("X") String X,
        @JsonProperty("Y") String Y
    )
    {
      this.title = title;
      this.width = width == null ? 500 : width;
      this.height = height == null ? 500 : height;
      this.X = X;
      this.Y = Y;
    }

    @Override
    public String contentType()
    {
      return "image/png";
    }

    @Override
    public Writer createFormatter(
        final OutputStream os,
        final ObjectMapper mapper,
        final RelDataType rowType
    ) throws IOException
    {
      final List<String> columnNames = RowSignature.columnNames(rowType);
      final String xAxis;
      final String yAxis;
      final int ix;
      final int iy;
      if (X == null && Y == null) {
        ix = 0;
        iy = 1;
        xAxis = columnNames.get(ix);
        yAxis = columnNames.get(iy);
      } else {
        xAxis = X;
        yAxis = Y;
        ix = columnNames.indexOf(X);
        iy = columnNames.indexOf(Y);
        Preconditions.checkArgument(ix >= 0, "Invalid x column name %s", X);
        Preconditions.checkArgument(iy >= 0, "Invalid y column name %s", Y);
      }
      return new Writer()
      {
        private final XYSeries dataset = new XYSeries("data");

        @Override
        public void writeRow(Object[] row) throws IOException
        {
          dataset.add((Number) row[ix], (Number) row[iy], false);
        }

        @Override
        public void end() throws IOException
        {
          XYSeriesCollection data = new XYSeriesCollection(dataset);
          JFreeChart chart = ChartFactory.createXYLineChart(
              title, xAxis, yAxis, data, PlotOrientation.VERTICAL, true, true, false
          );
          ChartUtils.writeChartAsPNG(os, chart, width, height);
        }
      };
    }
  }

  @JsonTypeName("scatter")
  class Scatter implements ResultFormat
  {
    private final String title;
    private final int width;
    private final int height;
    private final String X;
    private final String Y;

    @JsonCreator
    public Scatter(
        @JsonProperty("title") String title,
        @JsonProperty("width") Integer width,
        @JsonProperty("height") Integer height,
        @JsonProperty("X") String X,
        @JsonProperty("Y") String Y
    )
    {
      this.title = title;
      this.width = width == null ? 500 : width;
      this.height = height == null ? 500 : height;
      this.X = X;
      this.Y = Y;
    }

    @Override
    public String contentType()
    {
      return "image/png";
    }

    @Override
    public Writer createFormatter(
        final OutputStream os,
        final ObjectMapper mapper,
        final RelDataType rowType
    ) throws IOException
    {
      final List<String> columnNames = RowSignature.columnNames(rowType);
      final String xAxis;
      final String yAxis;
      final int ix;
      final int iy;
      if (X == null && Y == null) {
        ix = 0;
        iy = 1;
        xAxis = columnNames.get(ix);
        yAxis = columnNames.get(iy);
      } else {
        xAxis = X;
        yAxis = Y;
        ix = columnNames.indexOf(X);
        iy = columnNames.indexOf(Y);
        Preconditions.checkArgument(ix >= 0, "Invalid x column name %s", X);
        Preconditions.checkArgument(iy >= 0, "Invalid y column name %s", Y);
      }
      return new Writer()
      {
        private final Map<Number, List<double[]>> XY = Maps.newHashMap();

        @Override
        public void writeRow(Object[] row) throws IOException
        {
          XY.computeIfAbsent((Number) row[2], k -> Lists.newArrayList())
            .add(new double[]{((Number) row[0]).doubleValue(), ((Number) row[1]).doubleValue()});
        }

        @Override
        public void end() throws IOException
        {
          DefaultXYDataset dataset = new DefaultXYDataset();
          XYAreaRenderer renderer = new XYAreaRenderer();
          for (Map.Entry<Number, List<double[]>> entry : XY.entrySet()) {
            String seriesKey = String.valueOf(entry.getKey());
            dataset.addSeries(seriesKey, entry.getValue().toArray(new double[0][]));
            renderer.setSeriesPaint(dataset.indexOf(seriesKey), randomColor());
          }
          JFreeChart chart = ChartFactory.createScatterPlot(
              title, "X", "Y", dataset, PlotOrientation.VERTICAL, true, true, false);
          chart.setBackgroundPaint(Color.white);

          ((XYPlot) chart.getPlot()).setRenderer(renderer);

          ChartUtils.writeChartAsPNG(os, chart, width, height);
        }

        private final Random r = new Random(System.currentTimeMillis());

        private Color randomColor()
        {
          return new Color(r.nextInt(255), r.nextInt(255), r.nextInt(255), r.nextInt(255));
        }
      };
    }
  }
}
