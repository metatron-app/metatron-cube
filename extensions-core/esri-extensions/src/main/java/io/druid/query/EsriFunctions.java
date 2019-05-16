/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorCrosses;
import com.esri.core.geometry.OperatorDisjoint;
import com.esri.core.geometry.OperatorEquals;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.OperatorTouches;
import com.esri.core.geometry.OperatorWithin;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.esri.hadoop.hive.GeometryUtils.OGCType;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.json.JSONException;

import java.util.List;

import static io.druid.query.EsriUtils.OGC_GEOMETRY_TYPE;

/**
 */
public interface EsriFunctions extends Function.Library
{
  abstract class GeomFactory extends NamedFactory implements Function.TypeFixed
  {
    @Override
    public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
    {
      return OGC_GEOMETRY_TYPE;
    }
  }

  @Function.Named("ST_AsText")
  class ST_AsText extends NamedFactory.StringType
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(EsriUtils.toGeometry(Evals.eval(args.get(0), bindings)).asText());
        }
      };
    }
  }

  @Function.Named("ST_Buffer")
  class ST_Buffer extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry geometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ExprEval.of(null, OGC_GEOMETRY_TYPE);
          }
          double distance = Evals.evalDouble(args.get(1), bindings);
          if (args.size() > 2) {
            geometry.setSpatialReference(SpatialReference.create(Evals.evalInt(args.get(2), bindings)));
          }
          return ExprEval.of(geometry.buffer(distance), OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_GeomFromText")
  class ST_GeomFromText extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 1 || args.size() > 2) {
        throw new IAE("Function[%s] must have 1 or 2 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry geometry = OGCGeometry.fromText(Evals.evalString(args.get(0), bindings));
          if (args.size() > 1) {
            geometry.setSpatialReference(SpatialReference.create(Evals.evalInt(args.get(1), bindings)));
          }
          return ExprEval.of(geometry, OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_GeomFromGeoJson")
  class ST_GeomFromGeoJson extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 1 || args.size() > 2) {
        throw new IAE("Function[%s] must have 1 or 2 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          try {
            OGCGeometry geometry = OGCGeometry.fromGeoJson(Evals.evalString(args.get(0), bindings));
            if (args.size() > 1) {
              geometry.setSpatialReference(SpatialReference.create(Evals.evalInt(args.get(1), bindings)));
            }
            return ExprEval.of(geometry, OGC_GEOMETRY_TYPE);
          }
          catch (JSONException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
  }

  @Function.Named("ST_Point")
  class ST_Point extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 2 || args.size() > 4) {
        throw new IAE("Function[%s] must have 2 to 4 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Point point = new Point(Evals.evalDouble(args.get(0), bindings), Evals.evalDouble(args.get(1), bindings));
          if (args.size() > 2) {
            point.setZ(Evals.evalDouble(args.get(2), bindings));
          }
          if (args.size() > 3) {
            point.setM(Evals.evalDouble(args.get(3), bindings));
          }
          return ExprEval.of(OGCGeometry.createFromEsriGeometry(point, null), OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_Polygon")
  class ST_Polygon extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() == 1) {
        // from wkt
        return new Child()
        {
          @Override
          public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
          {
            OGCGeometry evaluate = EsriUtils.evaluate(Evals.evalString(args.get(0), bindings));
            if (evaluate != null) {
              final Geometry.Type type = evaluate.getEsriGeometry().getType();
              if (type != Geometry.Type.Polygon) {
                evaluate = null;
              }
            }
            return ExprEval.of(evaluate, OGC_GEOMETRY_TYPE);
          }
        };
      }
      if (args.size() < 6 || args.size() % 2 != 0) {
        throw new IAE("Function[%s] must have at least 6 & even numbered arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          double[] doubles = new double[args.size()];
          for (int i = 0; i < args.size(); i++) {
            doubles[i] = Evals.evalDouble(args.get(i), bindings);
          }
          return ExprEval.of(EsriUtils.toPolygon(doubles), OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_LineString")
  class ST_LineString extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() == 1) {
        // from wkt
        return new Child()
        {
          @Override
          public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
          {
            OGCGeometry evaluate = EsriUtils.evaluate(Evals.evalString(args.get(0), bindings));
            if (evaluate != null) {
              final Geometry.Type type = evaluate.getEsriGeometry().getType();
              if (type != Geometry.Type.Line || type != Geometry.Type.Polyline) {
                evaluate = null;
              }
            }
            return ExprEval.of(evaluate, OGC_GEOMETRY_TYPE);
          }
        };
      }
      if (args.isEmpty() || args.size() % 2 != 0) {
        throw new IAE("Function[%s] must have at least 2 & even numbered arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Polyline linestring = new Polyline();
          linestring.startPath(Evals.evalDouble(args.get(0), bindings), Evals.evalDouble(args.get(1), bindings));

          for (int i = 2; i < args.size(); i += 2) {
            linestring.lineTo(
                Evals.evalDouble(args.get(i), bindings),
                Evals.evalDouble(args.get(i + 1), bindings)
            );
          }
          return ExprEval.of(OGCGeometry.createFromEsriGeometry(linestring, null), OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_Area")
  class ST_Area extends NamedFactory.DoubleType
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have at 1 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (ogcGeometry == null) {
            return ExprEval.of(-1D);
          }
          return ExprEval.of(ogcGeometry.getEsriGeometry().calculateArea2D());
        }
      };
    }
  }

  @Function.Named("ST_Length")
  class ST_Length extends NamedFactory.DoubleType
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have at 1 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (ogcGeometry == null) {
            return ExprEval.of(-1D);
          }
          return ExprEval.of(ogcGeometry.getEsriGeometry().calculateLength2D());
        }
      };
    }
  }

  @Function.Named("ST_Centroid")
  class ST_Centroid extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {

          OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (ogcGeometry == null) {
            return ExprEval.of(null, OGC_GEOMETRY_TYPE);
          }

          OGCType ogcType = EsriUtils.ogcType(ogcGeometry);
          switch (ogcType) {
            case ST_MULTIPOLYGON:
            case ST_POLYGON:
              int wkid = ogcGeometry.SRID();
              SpatialReference spatialReference = null;
              if (wkid != GeometryUtils.WKID_UNKNOWN) {
                spatialReference = SpatialReference.create(wkid);
              }
              Envelope envBound = new Envelope();
              ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
              Point centroid = new Point(
                  (envBound.getXMin() + envBound.getXMax()) / 2.,
                  (envBound.getYMin() + envBound.getYMax()) / 2.
              );
              return ExprEval.of(
                  OGCGeometry.createFromEsriGeometry(centroid, spatialReference),
                  OGC_GEOMETRY_TYPE
              );
            default:
              return ExprEval.of(null, OGC_GEOMETRY_TYPE);
          }
        }
      };
    }
  }

  @Function.Named("ST_SRID")
  class ST_SRID extends NamedFactory.LongType
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(EsriUtils.toGeometry(Evals.eval(args.get(0), bindings)).SRID());
        }
      };
    }
  }

  @Function.Named("ST_SetSRID")
  class ST_SetSRID extends GeomFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          Geometry esriGeometry = ogcGeometry.getEsriGeometry();
          SpatialReference esriRef = ogcGeometry.getEsriSpatialReference();

          int wkid = Evals.evalInt(args.get(1), bindings);
          if (esriRef == null || esriRef.getID() != wkid) {
            ogcGeometry = OGCGeometry.createFromEsriGeometry(esriGeometry, SpatialReference.create(wkid));
          }
          return ExprEval.of(ogcGeometry, OGC_GEOMETRY_TYPE);
        }
      };
    }
  }

  @Function.Named("ST_GeodesicLengthWGS84")
  class ST_GeodesicLengthWGS84 extends NamedFactory.DoubleType
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 arguments", name());
      }
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (ogcGeometry == null) {
            return ExprEval.of(0D);
          }
          Geometry esriGeom = ogcGeometry.getEsriGeometry();
          if (esriGeom.getType() == Geometry.Type.Point || esriGeom.getType() == Geometry.Type.MultiPoint) {
            return ExprEval.of(0D);
          }
          MultiPath lines = (MultiPath) esriGeom;
          int nPath = lines.getPathCount();
          double length = 0.;
          for (int ix = 0; ix < nPath; ix++) {
            int curPt = lines.getPathStart(ix);
            int pastPt = lines.getPathEnd(ix);
            Point fromPt = lines.getPoint(curPt);
            for (int vx = curPt + 1; vx < pastPt; vx++) {
              Point toPt = lines.getPoint(vx);
              length += GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
              fromPt = toPt;
            }
          }
          return ExprEval.of(length);
        }
      };
    }
  }

  abstract class ST_GeometryRelational extends NamedFactory.BooleanType
  {
    protected abstract OperatorSimpleRelation getRelationOperator();

    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have at 2 arguments", name());
      }
      return new Child()
      {
        final OperatorSimpleRelation relation = getRelationOperator();

        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry geom1 = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          OGCGeometry geom2 = EsriUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(false);
          }
          return ExprEval.of(
              relation.execute(
                  geom1.getEsriGeometry(),
                  geom2.getEsriGeometry(),
                  geom1.getEsriSpatialReference(),
                  null
              )
          );
        }
      };
    }
  }

  @Function.Named("ST_Overlaps")
  class ST_Overlaps extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorOverlaps.local();
    }
  }

  @Function.Named("ST_Within")
  class ST_Within extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorWithin.local();
    }
  }

  @Function.Named("ST_Equals")
  class ST_Equals extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorEquals.local();
    }
  }

  @Function.Named("ST_Contains")
  class ST_Contains extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorContains.local();
    }
  }

  @Function.Named("ST_Crosses")
  class ST_Crosses extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorCrosses.local();
    }
  }

  @Function.Named("ST_Intersects")
  class ST_Intersects extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorIntersects.local();
    }
  }

  @Function.Named("ST_Touches")
  class ST_Touches extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorTouches.local();
    }
  }

  @Function.Named("ST_Disjoints")
  class ST_Disjoints extends ST_GeometryRelational
  {
    @Override
    protected OperatorSimpleRelation getRelationOperator()
    {
      return OperatorDisjoint.local();
    }
  }

  @Function.Named("ST_Distance")
  class ST_Distance extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }

      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          OGCGeometry ogcGeom1 = EsriUtils.toGeometry(Evals.eval(args.get(0), bindings));
          OGCGeometry ogcGeom2 = EsriUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (ogcGeom1 == null || ogcGeom2 == null) {
            return ExprEval.of(-1D);
          }
          return ExprEval.of(ogcGeom1.distance(ogcGeom2));
        }
      };
    }
  }

  @Function.Named("ST_ConvexHull")
  class ST_ConvexHull extends GeomFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      return new Child()
      {
        @Override
        public ExprEval evlaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          int wkid = GeometryUtils.WKID_UNKNOWN;
          List<Geometry> geometries = Lists.newArrayList();
          for (Expr expr : args) {
            OGCGeometry ogcGeometry = EsriUtils.toGeometry(Evals.eval(expr, bindings));
            if (wkid != GeometryUtils.WKID_UNKNOWN && wkid != ogcGeometry.SRID()) {
              return ExprEval.of(null, OGC_GEOMETRY_TYPE);
            }
            geometries.add(ogcGeometry.getEsriGeometry());
            wkid = ogcGeometry.SRID();
          }
          Geometry[] convexHull = GeometryEngine.convexHull(geometries.toArray(new Geometry[0]), true);
          if (convexHull.length == 1) {
            SpatialReference reference = wkid == GeometryUtils.WKID_UNKNOWN ? null : SpatialReference.create(wkid);
            return ExprEval.of(
                OGCGeometry.createFromEsriGeometry(convexHull[0], reference), OGC_GEOMETRY_TYPE
            );
          }
          return ExprEval.of(null, OGC_GEOMETRY_TYPE);
        }
      };
    }
  }
}
