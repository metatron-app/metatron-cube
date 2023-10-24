// Licensed to SK Telecom Co., LTD. (SK Telecom) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// SK Telecom licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

grammar Expr;

start : expr EOF;

expr : ('-'|'!') expr                                 # unaryOpExpr
     |<assoc=right> expr '^' expr                     # powOpExpr
     | expr ('*'|'/'|'%') expr                        # mulDivModuloExpr
     | expr ('+'|'-') expr                            # addSubExpr
     | expr ('<'|'<='|'>'|'>='|'=='|'!=') expr        # logicalOpExpr
     | expr ('&&'|'||') expr                          # logicalAndOrExpr
     | expr '=' expr                                  # assignExpr
     | '(' expr ')'                                   # nestedExpr
     | 'NULL'                                         # string
     | BOOLEAN                                        # booleanExpr
     | IDENTIFIER ('.' IDENTIFIER)*                   # identifierExpr
     | IDENTIFIER '[' ('-')? LONG ']'                 # identifierExpr
     | IDENTIFIER '(' fnArgs ')'                      # functionExpr
     | DECIMAL                                        # decimalExpr
     | FLOAT                                          # floatExpr
     | DOUBLE                                         # doubleExpr
     | LONG                                           # longExpr
     | STRING                                         # string
     ;

fnArgs : expr? (',' expr?)*                           # functionArgs
     ;

BOOLEAN : TRUE | FALSE ;
fragment TRUE : [Tt] [Rr] [Uu] [Ee];
fragment FALSE : [Ff] [Aa] [Ll] [Ss] [Ee];

IDENTIFIER : [_$#a-zA-Z\uAC00-\uD7AF][._$#a-zA-Z0-9\[\]\uAC00-\uD7AF]* | '"' ~["]+ '"';
DECIMAL : [0-9]+ ('.' [0-9]*)? [bB] ;
FLOAT : [0-9]+ ('.' [0-9]*('E'[-+][1-9][0-9]*)?)? [fF] ;
DOUBLE : [0-9]+ ('.' [0-9]*('E'[-+][1-9][0-9]*)? [dD]? | [dD]) ;
LONG : [0-9]+ ;
WS : [ \t\r\n ]+ -> skip ;

STRING : '\'' (ESC | ~ [\'\\])* '\'';
fragment ESC : '\\' ([\'\\/bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;

MINUS : '-' ;
NOT : '!' ;
POW : '^' ;
MUL : '*' ;
DIV : '/' ;
MODULO : '%' ;
PLUS : '+' ;
LT : '<' ;
LEQ : '<=' ;
GT : '>' ;
GEQ : '>=' ;
EQ : '==' ;
NEQ : '!=' ;
AND : '&&' ;
OR : '||' ;
ASSIGN : '=' ;
