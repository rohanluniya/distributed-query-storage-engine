"""First-principles recursive-descent SQL lexer and parser for the DQSE analytical subset.

Supported grammar:
  [EXPLAIN [ANALYZE]]
  SELECT <select_item> [, ...]
  FROM <table> [AS <alias>]
  [INNER JOIN <table> [AS <alias>] ON <col> = <col>]*
  [WHERE <bool_expr>]
  [GROUP BY <col> [, ...]]
  [ORDER BY <expr> [ASC|DESC] [, ...]]
  [LIMIT <int>]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class SQLParseError(ValueError):
    """Raised when SQL input violates the supported analytical SQL grammar."""


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    pos: int


KEYWORDS = {
    "SELECT",
    "FROM",
    "WHERE",
    "INNER",
    "JOIN",
    "ON",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "BETWEEN",
    "GROUP",
    "BY",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "EXPLAIN",
    "ANALYZE",
    "TRUE",
    "FALSE",
    "NULL",
}

AGG_FUNCTIONS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}


def tokenize(sql: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            while i < n and sql[i] != "\n":
                i += 1
            continue
        if ch == "'":
            start = i
            i += 1
            buf: list[str] = []
            while i < n:
                if sql[i] == "'" and i + 1 < n and sql[i + 1] == "'":
                    buf.append("'")
                    i += 2
                elif sql[i] == "'":
                    i += 1
                    break
                else:
                    buf.append(sql[i])
                    i += 1
            else:
                raise SQLParseError(f"Unterminated string literal at position {start}")
            tokens.append(Token("STRING", "".join(buf), start))
            continue
        if ch.isdigit() or (ch == "." and i + 1 < n and sql[i + 1].isdigit()):
            start = i
            has_dot = False
            while i < n and (sql[i].isdigit() or (sql[i] == "." and not has_dot)):
                if sql[i] == ".":
                    has_dot = True
                i += 1
            tokens.append(Token("FLOAT" if has_dot else "INT", sql[start:i], start))
            continue
        if ch.isalpha() or ch == "_":
            start = i
            while i < n and (sql[i].isalnum() or sql[i] == "_"):
                i += 1
            word = sql[start:i]
            upper = word.upper()
            if upper in KEYWORDS:
                tokens.append(Token("KW", upper, start))
            else:
                tokens.append(Token("IDENT", word, start))
            continue
        two = sql[i : i + 2]
        if two in ("!=", "<>", "<=", ">="):
            tokens.append(Token("OP", "!=" if two == "<>" else two, i))
            i += 2
            continue
        if ch in ("=", "<", ">", "+", "-", "*", "/"):
            tokens.append(Token("OP", ch, i))
            i += 1
            continue
        if ch in ("(", ")", ",", ".", ";"):
            tokens.append(Token(ch, ch, i))
            i += 1
            continue
        raise SQLParseError(f"Unexpected character '{ch}' at position {i}")
    tokens.append(Token("EOF", "", n))
    return tokens


# --- AST Nodes ---


@dataclass(frozen=True)
class Expr:
    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class Literal(Expr):
    value: Any

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "literal", "value": self.value}


@dataclass(frozen=True)
class ColumnRef(Expr):
    column: str
    table: str | None = None

    def full_name(self) -> str:
        return f"{self.table}.{self.column}" if self.table else self.column

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "col", "table": self.table, "column": self.column}


@dataclass(frozen=True)
class BinaryOp(Expr):
    op: str
    left: Expr
    right: Expr

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "binary",
            "op": self.op,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }


@dataclass(frozen=True)
class UnaryOp(Expr):
    op: str
    operand: Expr

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "unary", "op": self.op, "operand": self.operand.to_dict()}


@dataclass(frozen=True)
class InExpr(Expr):
    expr: Expr
    values: tuple[Literal, ...]
    negated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "in",
            "expr": self.expr.to_dict(),
            "values": [v.value for v in self.values],
            "negated": self.negated,
        }


@dataclass(frozen=True)
class BetweenExpr(Expr):
    expr: Expr
    low: Literal
    high: Literal
    negated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "between",
            "expr": self.expr.to_dict(),
            "low": self.low.value,
            "high": self.high.value,
            "negated": self.negated,
        }


@dataclass(frozen=True)
class AggCall(Expr):
    func: str  # COUNT, SUM, AVG, MIN, MAX
    arg: Expr | None  # None represents COUNT(*)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "agg",
            "func": self.func,
            "arg": self.arg.to_dict() if self.arg is not None else None,
        }


@dataclass(frozen=True)
class SelectItem:
    expr: Expr
    alias: str | None = None


@dataclass(frozen=True)
class TableRef:
    name: str
    alias: str | None = None

    @property
    def ref_name(self) -> str:
        return self.alias or self.name


@dataclass(frozen=True)
class JoinClause:
    table: TableRef
    left_key: ColumnRef
    right_key: ColumnRef


@dataclass(frozen=True)
class OrderByItem:
    expr: Expr
    ascending: bool = True


@dataclass(frozen=True)
class SelectQuery:
    select_items: tuple[SelectItem, ...]
    from_table: TableRef
    joins: tuple[JoinClause, ...] = ()
    where: Expr | None = None
    group_by: tuple[ColumnRef, ...] = ()
    order_by: tuple[OrderByItem, ...] = ()
    limit: int | None = None
    is_explain: bool = False
    is_analyze: bool = False


def expr_from_dict(data: dict[str, Any]) -> Expr:
    kind = data["kind"]
    if kind == "literal":
        return Literal(data["value"])
    if kind == "col":
        return ColumnRef(column=data["column"], table=data.get("table"))
    if kind == "binary":
        return BinaryOp(
            op=data["op"],
            left=expr_from_dict(data["left"]),
            right=expr_from_dict(data["right"]),
        )
    if kind == "unary":
        return UnaryOp(op=data["op"], operand=expr_from_dict(data["operand"]))
    if kind == "in":
        return InExpr(
            expr=expr_from_dict(data["expr"]),
            values=tuple(Literal(v) for v in data["values"]),
            negated=bool(data.get("negated", False)),
        )
    if kind == "between":
        return BetweenExpr(
            expr=expr_from_dict(data["expr"]),
            low=Literal(data["low"]),
            high=Literal(data["high"]),
            negated=bool(data.get("negated", False)),
        )
    if kind == "agg":
        return AggCall(
            func=data["func"],
            arg=expr_from_dict(data["arg"]) if data.get("arg") is not None else None,
        )
    raise ValueError(f"Unknown expr kind: {kind}")


def collect_columns(expr: Expr) -> set[ColumnRef]:
    if isinstance(expr, ColumnRef):
        return {expr}
    if isinstance(expr, Literal):
        return set()
    if isinstance(expr, BinaryOp):
        return collect_columns(expr.left) | collect_columns(expr.right)
    if isinstance(expr, UnaryOp):
        return collect_columns(expr.operand)
    if isinstance(expr, InExpr):
        return collect_columns(expr.expr)
    if isinstance(expr, BetweenExpr):
        return collect_columns(expr.expr)
    if isinstance(expr, AggCall):
        return collect_columns(expr.arg) if expr.arg is not None else set()
    return set()


def contains_agg(expr: Expr) -> bool:
    if isinstance(expr, AggCall):
        return True
    if isinstance(expr, BinaryOp):
        return contains_agg(expr.left) or contains_agg(expr.right)
    if isinstance(expr, UnaryOp):
        return contains_agg(expr.operand)
    return False


def format_expr(expr: Expr) -> str:
    if isinstance(expr, Literal):
        return f"'{expr.value}'" if isinstance(expr.value, str) else str(expr.value)
    if isinstance(expr, ColumnRef):
        return expr.full_name()
    if isinstance(expr, BinaryOp):
        return f"({format_expr(expr.left)} {expr.op} {format_expr(expr.right)})"
    if isinstance(expr, UnaryOp):
        return f"({expr.op} {format_expr(expr.operand)})"
    if isinstance(expr, InExpr):
        neg = "NOT " if expr.negated else ""
        vals = ", ".join(format_expr(v) for v in expr.values)
        return f"({format_expr(expr.expr)} {neg}IN ({vals}))"
    if isinstance(expr, BetweenExpr):
        neg = "NOT " if expr.negated else ""
        return f"({format_expr(expr.expr)} {neg}BETWEEN {format_expr(expr.low)} AND {format_expr(expr.high)})"
    if isinstance(expr, AggCall):
        inner = "*" if expr.arg is None else format_expr(expr.arg)
        return f"{expr.func}({inner})"
    return str(expr)


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.idx = 0

    def peek(self, offset: int = 0) -> Token:
        pos = min(self.idx + offset, len(self.tokens) - 1)
        return self.tokens[pos]

    def consume(self, kind: str | None = None, value: str | None = None) -> Token:
        tok = self.peek()
        if kind is not None and tok.kind != kind:
            raise SQLParseError(f"Expected {kind} but found {tok.kind} ('{tok.value}') at pos {tok.pos}")
        if value is not None and tok.value != value:
            raise SQLParseError(f"Expected '{value}' but found '{tok.value}' at pos {tok.pos}")
        self.idx += 1
        return tok

    def match_kw(self, kw: str) -> bool:
        tok = self.peek()
        if tok.kind == "KW" and tok.value == kw:
            self.idx += 1
            return True
        return False

    def parse_statement(self) -> SelectQuery:
        is_explain = False
        is_analyze = False
        if self.match_kw("EXPLAIN"):
            is_explain = True
            if self.match_kw("ANALYZE"):
                is_analyze = True

        self.consume("KW", "SELECT")
        select_items = self._parse_select_list()
        self.consume("KW", "FROM")
        from_table = self._parse_table_ref()

        joins: list[JoinClause] = []
        while self.peek().kind == "KW" and self.peek().value in ("INNER", "JOIN"):
            if self.peek().value == "INNER":
                self.consume("KW", "INNER")
            self.consume("KW", "JOIN")
            join_tbl = self._parse_table_ref()
            self.consume("KW", "ON")
            left_col = self._parse_column_ref()
            self.consume("OP", "=")
            right_col = self._parse_column_ref()
            joins.append(JoinClause(table=join_tbl, left_key=left_col, right_key=right_col))

        where_expr: Expr | None = None
        if self.match_kw("WHERE"):
            where_expr = self._parse_or()

        group_by: list[ColumnRef] = []
        if self.match_kw("GROUP"):
            self.consume("KW", "BY")
            group_by.append(self._parse_column_ref())
            while self.peek().kind == ",":
                self.consume(",")
                group_by.append(self._parse_column_ref())

        order_by: list[OrderByItem] = []
        if self.match_kw("ORDER"):
            self.consume("KW", "BY")
            while True:
                expr = self._parse_additive()
                asc = True
                if self.match_kw("ASC"):
                    asc = True
                elif self.match_kw("DESC"):
                    asc = False
                order_by.append(OrderByItem(expr=expr, ascending=asc))
                if self.peek().kind == ",":
                    self.consume(",")
                else:
                    break

        limit: int | None = None
        if self.match_kw("LIMIT"):
            tok = self.consume("INT")
            limit = int(tok.value)
            if limit < 0:
                raise SQLParseError("LIMIT must be non-negative")

        if self.peek().kind == ";":
            self.consume(";")
        if self.peek().kind != "EOF":
            tok = self.peek()
            raise SQLParseError(f"Unexpected trailing token '{tok.value}' at pos {tok.pos}")

        return SelectQuery(
            select_items=tuple(select_items),
            from_table=from_table,
            joins=tuple(joins),
            where=where_expr,
            group_by=tuple(group_by),
            order_by=tuple(order_by),
            limit=limit,
            is_explain=is_explain,
            is_analyze=is_analyze,
        )

    def _parse_select_list(self) -> list[SelectItem]:
        items: list[SelectItem] = []
        while True:
            expr = self._parse_additive()
            alias: str | None = None
            if self.match_kw("AS") or self.peek().kind == "IDENT":
                alias = self.consume("IDENT").value
            items.append(SelectItem(expr=expr, alias=alias))
            if self.peek().kind == ",":
                self.consume(",")
            else:
                break
        return items

    def _parse_table_ref(self) -> TableRef:
        name = self.consume("IDENT").value.lower()
        alias: str | None = None
        if self.match_kw("AS") or self.peek().kind == "IDENT":
            alias = self.consume("IDENT").value.lower()
        return TableRef(name=name, alias=alias)

    def _parse_column_ref(self) -> ColumnRef:
        first = self.consume("IDENT").value.lower()
        if self.peek().kind == ".":
            self.consume(".")
            second = self.consume("IDENT").value.lower()
            return ColumnRef(column=second, table=first)
        return ColumnRef(column=first, table=None)

    def _parse_or(self) -> Expr:
        left = self._parse_and()
        while self.match_kw("OR"):
            right = self._parse_and()
            left = BinaryOp("OR", left, right)
        return left

    def _parse_and(self) -> Expr:
        left = self._parse_not()
        while self.match_kw("AND"):
            right = self._parse_not()
            left = BinaryOp("AND", left, right)
        return left

    def _parse_not(self) -> Expr:
        if self.match_kw("NOT"):
            return UnaryOp("NOT", self._parse_not())
        return self._parse_comparison()

    def _parse_comparison(self) -> Expr:
        left = self._parse_additive()
        negated = False
        if self.peek().kind == "KW" and self.peek().value == "NOT":
            nxt = self.peek(1)
            if nxt.kind == "KW" and nxt.value in ("IN", "BETWEEN"):
                self.consume("KW", "NOT")
                negated = True

        if self.match_kw("IN"):
            self.consume("(")
            vals: list[Literal] = []
            while True:
                vals.append(self._parse_literal())
                if self.peek().kind == ",":
                    self.consume(",")
                else:
                    break
            self.consume(")")
            return InExpr(expr=left, values=tuple(vals), negated=negated)

        if self.match_kw("BETWEEN"):
            low = self._parse_literal()
            self.consume("KW", "AND")
            high = self._parse_literal()
            return BetweenExpr(expr=left, low=low, high=high, negated=negated)

        if self.peek().kind == "OP" and self.peek().value in ("=", "!=", "<", "<=", ">", ">="):
            op = self.consume("OP").value
            right = self._parse_additive()
            return BinaryOp(op, left, right)
        return left

    def _parse_additive(self) -> Expr:
        left = self._parse_multiplicative()
        while self.peek().kind == "OP" and self.peek().value in ("+", "-"):
            op = self.consume("OP").value
            right = self._parse_multiplicative()
            left = BinaryOp(op, left, right)
        return left

    def _parse_multiplicative(self) -> Expr:
        left = self._parse_primary()
        while self.peek().kind == "OP" and self.peek().value in ("*", "/"):
            op = self.consume("OP").value
            right = self._parse_primary()
            left = BinaryOp(op, left, right)
        return left

    def _parse_literal(self) -> Literal:
        tok = self.peek()
        if tok.kind == "OP" and tok.value == "-":
            self.consume("OP", "-")
            num_tok = self.peek()
            if num_tok.kind == "INT":
                self.consume("INT")
                return Literal(-int(num_tok.value))
            if num_tok.kind == "FLOAT":
                self.consume("FLOAT")
                return Literal(-float(num_tok.value))
            raise SQLParseError(f"Expected number after unary '-' at pos {num_tok.pos}")
        if tok.kind == "INT":
            self.consume("INT")
            return Literal(int(tok.value))
        if tok.kind == "FLOAT":
            self.consume("FLOAT")
            return Literal(float(tok.value))
        if tok.kind == "STRING":
            self.consume("STRING")
            return Literal(tok.value)
        if tok.kind == "KW" and tok.value in ("TRUE", "FALSE"):
            self.consume("KW")
            return Literal(tok.value == "TRUE")
        raise SQLParseError(f"Expected literal but got '{tok.value}' at pos {tok.pos}")

    def _parse_primary(self) -> Expr:
        tok = self.peek()
        if tok.kind == "(":
            self.consume("(")
            inner = self._parse_or()
            self.consume(")")
            return inner
        if tok.kind == "OP" and tok.value == "-":
            self.consume("OP", "-")
            nxt = self.peek()
            if nxt.kind in ("INT", "FLOAT"):
                if nxt.kind == "INT":
                    self.consume("INT")
                    return Literal(-int(nxt.value))
                self.consume("FLOAT")
                return Literal(-float(nxt.value))
            return UnaryOp("-", self._parse_primary())
        if tok.kind in ("INT", "FLOAT", "STRING") or (
            tok.kind == "KW" and tok.value in ("TRUE", "FALSE")
        ):
            return self._parse_literal()
        if tok.kind == "IDENT":
            upper = tok.value.upper()
            if upper in AGG_FUNCTIONS and self.peek(1).kind == "(":
                self.consume("IDENT")
                self.consume("(")
                if upper == "COUNT" and self.peek().kind == "OP" and self.peek().value == "*":
                    self.consume("OP", "*")
                    self.consume(")")
                    return AggCall(func="COUNT", arg=None)
                arg = self._parse_additive()
                self.consume(")")
                return AggCall(func=upper, arg=arg)
            return self._parse_column_ref()
        raise SQLParseError(f"Unexpected token {tok.kind} ('{tok.value}') at pos {tok.pos}")


def parse_sql(sql: str) -> SelectQuery:
    """Parse an analytical SQL statement into a typed SelectQuery AST."""
    tokens = tokenize(sql.strip())
    return _Parser(tokens).parse_statement()
