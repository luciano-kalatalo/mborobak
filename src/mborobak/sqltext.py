"""Light scanning of user SQL for ``session.sql()`` (API contract §3.5).

This is not a parser. It finds tokens outside strings, quoted identifiers and
comments, tracks parenthesis depth, and answers the few questions needed to
use a user's SELECT as a subquery in IRIS:

* IRIS rejects ``WITH`` inside a subquery, so CTEs are split off and later
  placed in front of the final statement.
* IRIS rejects ``ORDER BY`` in a subquery without ``TOP``, so ``TOP ALL`` is
  added (compiler spec §7.2). A ``UNION … ORDER BY`` can't take ``TOP ALL``,
  so it is wrapped as ``SELECT TOP ALL * FROM (union) ORDER BY …``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import UnsupportedOperationError

_WORD = re.compile(r"[A-Za-z_%][A-Za-z0-9_$%]*")
_NAMED = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class Token:
    kind: str  # word, param (?), named (:name), semicolon, open, close, other
    text: str
    start: int
    depth: int


def tokens(sql: str) -> list[Token]:
    out: list[Token] = []
    i, n, depth = 0, len(sql), 0
    while i < n:
        ch = sql[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                if sql[j] == "'":
                    break
                j += 1
            if j >= n:
                raise ValueError("unterminated string literal in SQL")
            i = j + 1
        elif ch == '"':
            j = sql.find('"', i + 1)
            if j < 0:
                raise ValueError("unterminated quoted identifier in SQL")
            out.append(Token("word", sql[i:j + 1], i, depth))
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j < 0:
                raise ValueError("unterminated comment in SQL")
            i = j + 2
        elif ch == "(":
            out.append(Token("open", ch, i, depth))
            depth += 1
            i += 1
        elif ch == ")":
            depth -= 1
            out.append(Token("close", ch, i, depth))
            i += 1
        elif ch == "?":
            out.append(Token("param", ch, i, depth))
            i += 1
        elif ch == ";":
            out.append(Token("semicolon", ch, i, depth))
            i += 1
        elif ch == ":" and _NAMED.match(sql, i) and not (i > 0 and sql[i - 1] == ":"):
            m = _NAMED.match(sql, i)
            out.append(Token("named", m.group(0), i, depth))
            i = m.end()
        else:
            m = _WORD.match(sql, i)
            if m:
                out.append(Token("word", m.group(0), i, depth))
                i = m.end()
            else:
                if not ch.isspace():
                    out.append(Token("other", ch, i, depth))
                i += 1
    if depth != 0:
        raise ValueError("unbalanced parentheses in SQL")
    return out


def bind_named(sql: str, args: dict) -> tuple[str, list]:
    """Spark's named parameter markers (``:name``) → ``?`` markers and their values, in text order. Markers
    inside strings, quoted identifiers and comments are left alone. A name may appear more than once."""
    if not all(isinstance(k, str) for k in args):
        raise TypeError("sql(args=...) takes a dict with str keys for :name markers, or a list for ? markers")
    toks = tokens(sql)
    if any(t.kind == "param" for t in toks):
        raise UnsupportedOperationError("sql() with a dict of args takes :name markers, not ?")
    named = [t for t in toks if t.kind == "named"]
    missing = sorted({t.text[1:] for t in named if t.text[1:] not in args})
    if missing:
        raise ValueError(f"sql(): no value for parameter(s) {', '.join(':' + m for m in missing)}")
    text, values, last = [], [], 0
    for t in named:
        text.append(sql[last:t.start])
        text.append("?")
        values.append(args[t.text[1:]])
        last = t.start + len(t.text)
    text.append(sql[last:])
    return "".join(text), values


@dataclass(frozen=True)
class PreparedQuery:
    original: str  # the user's statement, without a trailing semicolon
    cte: str | None  # "WITH a AS (...), b AS (...)" or None
    cte_params: int  # number of ? markers in the CTE part
    body: str  # the main SELECT, safe to use as a subquery
    params: int  # total ? markers


def prepare(sql: str) -> PreparedQuery:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql() takes a non-empty SELECT statement")
    text = sql.strip()
    toks = tokens(text)
    semis = [t for t in toks if t.kind == "semicolon"]
    if semis:
        if len(semis) > 1 or semis[0] is not toks[-1]:
            raise UnsupportedOperationError("sql() takes one statement; remove the extra ';'")
        text = text[:semis[0].start].rstrip()
        toks = toks[:-1]
    words = [t for t in toks if t.kind == "word"]
    if not words or words[0].text.upper() not in ("SELECT", "WITH"):
        raise UnsupportedOperationError("sql() only runs SELECT statements (or WITH … SELECT)")
    forbidden = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "GRANT", "REVOKE", "TRUNCATE"}
    for t in words:
        if t.depth == 0 and t.text.upper() in forbidden and t is not words[0]:
            raise UnsupportedOperationError(f"sql() only runs SELECT statements; found {t.text.upper()}")
    total_params = sum(1 for t in toks if t.kind == "param")
    named = next((t for t in toks if t.kind == "named"), None)
    if named is not None:
        raise ValueError(f"the query has a named parameter {named.text}; pass its value in sql(args={{...}})")

    cte, cte_params, main_start = None, 0, 0
    if words[0].text.upper() == "WITH":
        main = next((t for t in words[1:] if t.depth == 0 and t.text.upper() == "SELECT"), None)
        if main is None:
            raise UnsupportedOperationError("WITH must be followed by a SELECT")
        cte = text[:main.start].rstrip()
        cte_params = sum(1 for t in toks if t.kind == "param" and t.start < main.start)
        main_start = main.start
    body = text[main_start:]
    body_toks = [t for t in toks if t.start >= main_start]
    body = _make_subquery_safe(body, [Token(t.kind, t.text, t.start - main_start, t.depth) for t in body_toks])
    return PreparedQuery(text, cte, cte_params, body, total_params)


def _make_subquery_safe(body: str, toks: list[Token]) -> str:
    top = [t for t in toks if t.depth == 0 and t.kind == "word"]
    upper = [t.text.upper() for t in top]
    order_at = next((top[i].start for i in range(len(top) - 1)
                     if upper[i] == "ORDER" and upper[i + 1] == "BY"), None)
    if order_at is None:
        return body
    if any(u in ("UNION", "EXCEPT", "INTERSECT") for u in upper):
        inner, order = body[:order_at].rstrip(), body[order_at:]
        return f"SELECT TOP ALL * FROM ({inner}) AS mborobak_u {order}"
    # SELECT [ALL | DISTINCT] [TOP n] …: add TOP ALL unless a TOP is already there.
    i = 1
    if len(upper) > 1 and upper[1] in ("DISTINCT", "ALL"):
        i = 2
    if len(upper) > i and upper[i] == "TOP":
        return body
    insert_at = top[i - 1].start + len(top[i - 1].text)
    return body[:insert_at] + " TOP ALL" + body[insert_at:]
