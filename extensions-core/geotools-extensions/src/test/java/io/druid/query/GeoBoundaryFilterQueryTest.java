/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.data.ConstantQuery;
import io.druid.query.select.StreamQuery;
import io.druid.segment.ExprVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GeoBoundaryFilterQueryTest extends GeoToolsTestHelper
{
  final StreamQuery source = new Druids.SelectQueryBuilder()
      .dataSource("estate")
      .columns("gis.lat", "gis.lon", "gis.addr")
      .streaming();

  final StreamQuery boundary = new Druids.SelectQueryBuilder()
      .dataSource("seoul_roads")
      .virtualColumns(
          new ExprVirtualColumn("shape_fromWKT(geom)", "shape"),
          new ExprVirtualColumn("shape_toWKT(shape_buffer(shape, 100, endCapStyle=2))", "geom_buf"),
          new ExprVirtualColumn("shape_length(shape)", "length")
      )
      .columns("geom_buf", "name", "length")
      .streaming();

  @Test
  public void test() throws Exception
  {
    List<Object[]> roadSides = runQuery(boundary);
    Assert.assertEquals(4, roadSides.size());

    Object[] road1 = roadSides.get(0);
    Assert.assertEquals("강남대로", road1[1]);
    Assert.assertEquals(0.040744881965, ((Number) road1[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))",
        road1[0]
    );
    Object[] road2 = roadSides.get(1);
    Assert.assertEquals("서초대로", road2[1]);
    Assert.assertEquals(0.020906297831, ((Number) road2[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))",
        road2[0]
    );
    Object[] road3 = roadSides.get(2);
    Assert.assertEquals("테헤란로", road3[1]);
    Assert.assertEquals(0.040590914168, ((Number) road3[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02797350830815 37.497214730240735, 127.02732249169185 37.4985432638503, 127.06611049169186 37.51050615741942))",
        road3[0]
    );
    Object[] road4 = roadSides.get(3);
    Assert.assertEquals("방배로", road4[1]);
    Assert.assertEquals(0.027482519025, ((Number) road4[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))",
        road4[0]
    );
    GeoBoundaryFilterQuery filtered = new GeoBoundaryFilterQuery(
        source, "gis.coord", null, null, boundary, "geom_buf", null, null, null, null, false, null
    );
    ObjectMapper mapper = segmentWalker.getObjectMapper();
    String serialized = mapper.writeValueAsString(filtered);
    GeoBoundaryFilterQuery deserialized = mapper.readValue(serialized, GeoBoundaryFilterQuery.class);
    Assert.assertEquals(filtered, deserialized);

    List<Object[]> roadSideEstates = runQuery(filtered);
    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[37.496533, 126.9874409, 방배동 758-4 삼호2]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[37.4976528, 126.9868109, 방배동 757-3 삼호3]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌]", Arrays.toString(roadSideEstates.get(7)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움]", Arrays.toString(roadSideEstates.get(8)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(9)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(10)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(11)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(12)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(13)));

    filtered = filtered.withBoundaryJoin(Arrays.asList("name"));
    roadSideEstates = runQuery(filtered);
    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[37.496533, 126.9874409, 방배동 758-4 삼호2, 방배로]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[37.4976528, 126.9868109, 방배동 757-3 삼호3, 방배로]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모, 강남대로]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌, 강남대로]", Arrays.toString(roadSideEstates.get(7)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움, 강남대로]", Arrays.toString(roadSideEstates.get(8)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로]", Arrays.toString(roadSideEstates.get(9)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로]", Arrays.toString(roadSideEstates.get(10)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(11)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(12)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(13)));

    roadSideEstates = runQuery(filtered.withFlip(true));
    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[방배로, 37.496687, 126.9883971, 방배동 725 방배신삼호]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[방배로, 37.496687, 126.9883971, 방배동 725 방배신삼호]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[방배로, 37.496533, 126.9874409, 방배동 758-4 삼호2]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[방배로, 37.4976528, 126.9868109, 방배동 757-3 삼호3]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[방배로, 37.4945392, 126.9891208, 방배동 772-13 현대멤피스]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[방배로, 37.4945392, 126.9891208, 방배동 772-13 현대멤피스]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[강남대로, 37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[강남대로, 37.4864785, 127.0335393, 도곡동 952 대우디오빌]", Arrays.toString(roadSideEstates.get(7)));
    Assert.assertEquals("[강남대로, 37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움]", Arrays.toString(roadSideEstates.get(8)));
    Assert.assertEquals("[강남대로, 37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(9)));
    Assert.assertEquals("[강남대로, 37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(10)));
    Assert.assertEquals("[강남대로, 37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(11)));
    Assert.assertEquals("[강남대로, 37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(12)));
    Assert.assertEquals("[강남대로, 37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(13)));
  }

  @Test
  public void testBoundaryJoin()
  {
    List<String> boundaryJoin = ImmutableList.of("name", "geom_buf");
    GeoBoundaryFilterQuery filtered = new GeoBoundaryFilterQuery(
        source, "gis.coord", null, null, boundary, "geom_buf", true, boundaryJoin, null, null, false, null
    );

    // returns 2 geometry (union into thress polygon, no 테헤란로)
    List<Object[]> roadSideEstates = runQuery(filtered);

    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[37.496533, 126.9874409, 방배동 758-4 삼호2, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[37.4976528, 126.9868109, 방배동 757-3 삼호3, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(7)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(8)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(9)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(10)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(11)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(12)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로, POLYGON ((127.02721222719981 37.49698187028977, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02657103360589 37.49831341868377, 127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.02824641744617 37.498828218616325, 127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02888761881577 37.49749666283049, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02721222719981 37.49698187028977))]", Arrays.toString(roadSideEstates.get(13)));

    // returns 3 geometry
    roadSideEstates = runQuery(filtered.withBoundaryUnion(false));

    int i = 0;
    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모, 강남대로, POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌, 강남대로, POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움, 강남대로, POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 서초대로, POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 서초대로, POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 서초대로, POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 서초대로, POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 서초대로, POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.496533, 126.9874409, 방배동 758-4 삼호2, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4976528, 126.9868109, 방배동 757-3 삼호3, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i++)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로, POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))]", Arrays.toString(roadSideEstates.get(i)));
  }

  @Test
  public void testOnConstant() throws Exception
  {
    ConstantQuery constant = new ConstantQuery(
        Arrays.asList("name", "geom_buf"),
        Arrays.<Object[]>asList(
            new Object[] {
                "강남대로",
                "POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))"
            },
            new Object[] {
                "서초대로",
                "POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))"
            },
            new Object[] {
                "테헤란로",
                "POLYGON ((127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02797350830815 37.497214730240735, 127.02732249169185 37.4985432638503, 127.06611049169186 37.51050615741942))"
            },
            new Object[] {
                "방배로",
                "POLYGON ((127.00265861569268 37.47544532894693, 127.00105738430733 37.47479866965426, 126.9862213843073 37.4979327697642, 126.98782261569266 37.49857922883665, 127.00265861569268 37.47544532894693))"
            }
        )
    );
    ObjectMapper mapper = segmentWalker.getObjectMapper();
    String serialized = mapper.writeValueAsString(constant);
    Query.ArrayOutputSupport deserialized = mapper.readValue(serialized, Query.ArrayOutputSupport.class);
    Assert.assertEquals(constant, deserialized);

    List<String> boundaryJoin = ImmutableList.of("name");
    GeoBoundaryFilterQuery filtered = new GeoBoundaryFilterQuery(
        source, "gis.coord", null, null, constant, "geom_buf", null, boundaryJoin, null, null, false, null
    );
    List<Object[]> roadSideEstates = runQuery(filtered);
    Assert.assertEquals(14, roadSideEstates.size());
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[37.496687, 126.9883971, 방배동 725 방배신삼호, 방배로]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[37.496533, 126.9874409, 방배동 758-4 삼호2, 방배로]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[37.4976528, 126.9868109, 방배동 757-3 삼호3, 방배로]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[37.4945392, 126.9891208, 방배동 772-13 현대멤피스, 방배로]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모, 강남대로]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌, 강남대로]", Arrays.toString(roadSideEstates.get(7)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움, 강남대로]", Arrays.toString(roadSideEstates.get(8)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로]", Arrays.toString(roadSideEstates.get(9)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온, 강남대로]", Arrays.toString(roadSideEstates.get(10)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(11)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(12)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥, 강남대로]", Arrays.toString(roadSideEstates.get(13)));
  }
}