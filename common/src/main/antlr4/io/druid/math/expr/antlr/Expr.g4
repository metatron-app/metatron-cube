grammar Expr;

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
     | IDENTIFIER '(' fnArgs ')'                      # functionExpr
     | IDENTIFIER                                     # identifierExpr
     | IDENTIFIER '[' ('-')? LONG ']'                 # identifierExpr
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
LONG : [0-9]+ ;
DOUBLE : [0-9]+ ('.' [0-9]* | [dD]) ;
FLOAT : [0-9]+ ('.' [0-9]*)? [fF] ;
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
