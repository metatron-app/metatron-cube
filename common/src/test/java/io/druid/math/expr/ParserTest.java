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

package io.druid.math.expr;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ParserTest
{
  @Test
  public void testSimple()
  {
    Assert.assertEquals("1", Parser.parse("1").toString());
    Assert.assertTrue(Parser.parse("concat(x)") instanceof FunctionExpr);
    Assert.assertTrue(Parser.parse("\"xx-yy(x)\"") instanceof IdentifierExpr);
  }

  @Test
  public void testNumbers()
  {
    Assert.assertTrue(Parser.parse("1") instanceof LongConst);
    Assert.assertTrue(Parser.parse("1f") instanceof FloatConst);
    Assert.assertTrue(Parser.parse("1F") instanceof FloatConst);
    Assert.assertTrue(Parser.parse("1d") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1D") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.0") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.0d") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.0D") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1b") instanceof DecimalConst);
    Assert.assertTrue(Parser.parse("1B") instanceof DecimalConst);
    Assert.assertTrue(Parser.parse("1.00000000000001") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.00000000000001f") instanceof FloatConst);

    Assert.assertTrue(Parser.parse("1.23E+3F") instanceof FloatConst);
    Assert.assertTrue(Parser.parse("1.23E-8F") instanceof FloatConst);
    Assert.assertTrue(Parser.parse("-1.23E-10F") instanceof FloatConst);
    Assert.assertTrue(Parser.parse("3.881417214870453E-8F") instanceof FloatConst);

    Assert.assertTrue(Parser.parse("1.23E+3") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.23E-8") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("-1.23E-10") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("3.881417214870453E-8") instanceof DoubleConst);

    Assert.assertTrue(Parser.parse("1.23E+3D") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("1.23E-8D") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("-1.23E-10D") instanceof DoubleConst);
    Assert.assertTrue(Parser.parse("3.881417214870453E-8D") instanceof DoubleConst);
  }

  @Test
  public void testUnicode()
  {
    Assert.assertTrue(Parser.parse("한글") instanceof IdentifierExpr);
    Assert.assertEquals("한글", Parser.parse("한글").toString());

    Assert.assertTrue(Parser.parse("\"한글\"") instanceof IdentifierExpr);
    Assert.assertEquals("한글", Parser.parse("\"한글\"").toString());

    Assert.assertTrue(Parser.parse("한글.나비스")  instanceof IdentifierExpr);
    Assert.assertEquals("한글.나비스", Parser.parse("한글.나비스").toString());

    Assert.assertTrue(Parser.parse("\"한글.나비스\"")  instanceof IdentifierExpr);
    Assert.assertEquals("한글.나비스", Parser.parse("\"한글.나비스\"").toString());

    Assert.assertTrue(Parser.parse("'한글.나비스'")  instanceof StringConst);
    Assert.assertEquals("한글.나비스", Parser.parse("'한글.나비스'").toString());

    Assert.assertTrue(Parser.parse("'\\u0001'") instanceof StringConst);
    Assert.assertEquals("\u0001", Parser.parse("'\\u0001'").toString());
  }

  @Test
  public void testNull()
  {
    Expr expr = Parser.parse("NULL");
    Assert.assertTrue(expr instanceof StringConst);
    Assert.assertNull(((StringConst)expr).get());
  }

  @Test
  public void testSimpleUnaryOps1()
  {
    String actual = Parser.parse("-x").toString();
    String expected = "-x";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("!x").toString();
    expected = "!x";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleUnaryOps2()
  {
    Assert.assertEquals("-1", Parser.parse("-1", false).toString());
    Assert.assertEquals("-1", Parser.parse("-1", true).toString());

    Assert.assertEquals("--1", Parser.parse("--1", false).toString());
    Assert.assertEquals("1", Parser.parse("--1", true).toString());

    Assert.assertEquals("(-1 + 2)", Parser.parse("-1+2", false).toString());
    Assert.assertEquals("1", Parser.parse("-1+2", true).toString());

    Assert.assertEquals("(-1 * 2)", Parser.parse("-1*2", false).toString());
    Assert.assertEquals("-2", Parser.parse("-1*2", true).toString());

    Assert.assertEquals("(-1 ^ 2)", Parser.parse("-1^2", false).toString());
    Assert.assertEquals("1", Parser.parse("-1^2", true).toString());
  }

  private void validateParser(String expression, String expected, String identifiers)
  {
    Assert.assertEquals(expected, Parser.parse(expression).toString());
    Assert.assertEquals(identifiers, Parser.findRequiredBindings(expression).toString());
  }

  @Test
  public void testSimpleLogicalOps1()
  {
    validateParser("x>y", "(x > y)", "[x, y]");
    validateParser("x<y", "(x < y)", "[x, y]");
    validateParser("x<=y", "(x <= y)", "[x, y]");
    validateParser("x>=y", "(x >= y)", "[x, y]");
    validateParser("x==y", "(x == y)", "[x, y]");
    validateParser("x!=y", "(x != y)", "[x, y]");
    validateParser("x && y", "(x && y)", "[x, y]");
    validateParser("x || y", "(x || y)", "[x, y]");
  }

  @Test
  public void testSimpleAdditivityOp1()
  {
    validateParser("x+y", "(x + y)", "[x, y]");
    validateParser("x-y", "(x - y)", "[x, y]");
  }

  @Test
  public void testSimpleAdditivityOp2()
  {
    validateParser("x+y+z", "((x + y) + z)", "[x, y, z]");
    validateParser("x+y-z", "((x + y) - z)", "[x, y, z]");
    validateParser("x-y+z", "((x - y) + z)", "[x, y, z]");
    validateParser("x-y-z", "((x - y) - z)", "[x, y, z]");
  }

  @Test
  public void testSimpleMultiplicativeOp1()
  {
    validateParser("x*y", "(x * y)", "[x, y]");
    validateParser("x/y", "(x / y)", "[x, y]");
    validateParser("x%y", "(x % y)", "[x, y]");
  }

  @Test
  public void testSimpleMultiplicativeOp2()
  {
    String actual = Parser.parse("1*2*3", false).toString();
    Assert.assertEquals("((1 * 2) * 3)", actual);

    actual = Parser.parse("1*2/3", false).toString();
    Assert.assertEquals("((1 * 2) / 3)", actual);

    actual = Parser.parse("1/2*3", false).toString();
    Assert.assertEquals("((1 / 2) * 3)", actual);

    actual = Parser.parse("1/2/3", false).toString();
    Assert.assertEquals("((1 / 2) / 3)", actual);
  }

  @Test
  public void testSimpleCarrot1()
  {
    testFlatten("1^2", "(1 ^ 2)", "1");
  }

  @Test
  public void testSimpleCarrot2()
  {
    testFlatten("1^2^3", "(1 ^ (2 ^ 3))", "1");
  }

  @Test
  public void testMixed()
  {
    testFlatten("1+2*3", "(1 + (2 * 3))", "7");
    testFlatten("1+(2*3)", "(1 + (2 * 3))", "7");
    testFlatten("(1+2)*3", "((1 + 2) * 3)", "9");

    testFlatten("1*2+3", "((1 * 2) + 3)", "5");
    testFlatten("(1*2)+3", "((1 * 2) + 3)", "5");
    testFlatten("1*(2+3)", "(1 * (2 + 3))", "5");

    testFlatten("1+2^3", "(1 + (2 ^ 3))", "9");
    testFlatten("1+(2^3)", "(1 + (2 ^ 3))", "9");
    testFlatten("(1+2)^3", "((1 + 2) ^ 3)", "27");

    testFlatten("1^2+3", "((1 ^ 2) + 3)", "4");
    testFlatten("(1^2)+3", "((1 ^ 2) + 3)", "4");
    testFlatten("1^(2+3)", "(1 ^ (2 + 3))", "1");

    testFlatten("1^2*3+4", "(((1 ^ 2) * 3) + 4)", "7");
    testFlatten("-1^2*-3+-4", "(((-1 ^ 2) * -3) + -4)", "-7");
  }

  private void testFlatten(String expr, String expected1, String expected2)
  {
    Assert.assertEquals(expected1, Parser.parse(expr, false).toString());
    Assert.assertEquals(expected2, Parser.parse(expr).toString());
  }

  @Test
  public void testFunctions()
  {
    validateParser("sqrt(x)", "(sqrt [x])", "[x]");
    validateParser("if(cond,then,else)", "(if [cond, then, else])", "[cond, then, else]");

    validateParser(
        "if(tot_scrbr_cnt=='NULL'||tot_scrbr_cnt=='\\n',1,tot_scrbr_cnt)",
        "(if [((tot_scrbr_cnt == NULL) || (tot_scrbr_cnt == \n)), 1, tot_scrbr_cnt])",
        "[tot_scrbr_cnt]"
    );
    validateParser(
        "if(tot_scrbr_cnt=='NULL'||tot_scrbr_cnt=='\\\\N',1,tot_scrbr_cnt)",
        "(if [((tot_scrbr_cnt == NULL) || (tot_scrbr_cnt == \\N)), 1, tot_scrbr_cnt])",
        "[tot_scrbr_cnt]"
    );
  }

  @Test
  public void testDecomposition()
  {
    Expr parse = Parser.parse("(a > 1 && a < 2) || (a > 100)");
    Assert.assertEquals("(((a > 1) && (a < 2)) || (a > 100))", parse.toString());
    Expr cnf = Expressions.convertToCNF(parse, Parser.EXPR_FACTORY);
    Assert.assertEquals("(((a > 100) || (a > 1)) && ((a > 100) || (a < 2)))", cnf.toString());

    parse = Parser.parse("!(a > 1 && a < 2) || (a > 100)");
    Assert.assertEquals("(!((a > 1) && (a < 2)) || (a > 100))", parse.toString());
    cnf = Expressions.convertToCNF(parse, Parser.EXPR_FACTORY);
    Assert.assertEquals("((!(a > 1) || !(a < 2)) || (a > 100))", cnf.toString());

    parse = Parser.parse("(a > 1 && a < 2) || !(a > 100)");
    Assert.assertEquals("(((a > 1) && (a < 2)) || !(a > 100))", parse.toString());
    cnf = Expressions.convertToCNF(parse, Parser.EXPR_FACTORY);
    Assert.assertEquals("((!(a > 100) || (a > 1)) && (!(a > 100) || (a < 2)))", cnf.toString());
  }
}
