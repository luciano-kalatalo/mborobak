"""SQL functions Mborobak creates in IRIS on first use (compiler spec §7.16).

IRIS SQL has no regular expressions, but ObjectScript has ICU's (``$LOCATE``, ``%Regex.Matcher``) and
``CREATE FUNCTION … LANGUAGE OBJECTSCRIPT`` makes them callable from SQL without a class file (probed on
2026.2). The functions live in the schema ``Mborobak``; their names carry a version, so a later Mborobak
that changes one creates a new name instead of altering a function an older Mborobak still calls.

Inside a function, a SQL NULL arrives as ``""`` and an empty string as ``$c(0)``. An empty pattern, which ICU
rejects and Java matches everywhere, becomes ``(?:)``; returning ``""`` gives NULL
and ``$c(0)`` gives ``''``. Compiled patterns are cached per connection in ``%mborobakRx``, which makes a
filter over 100,000 rows about ten times faster than compiling the pattern for every row.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .errors import MborobakExecutionError

if TYPE_CHECKING:
    from .session import MborobakSession

SCHEMA = "Mborobak"

# The pattern's compiled matcher, from the connection's cache. Sets m; quits with NULL on a NULL argument.
_MATCHER = """
 quit:(s="")||(p="") ""
 set:s=$c(0) s="" set:p=$c(0) p="(?:)"
 set k=$select($length(p)<=200:"."_p,1:"#"_$zcrc(p,7)_"."_$length(p))
 if $get(%mborobakRx(k,"p"),$c(1))'=p {
  if $increment(%mborobakRx)>200 { kill %mborobakRx }
  set %mborobakRx(k)=##class(%Regex.Matcher).%New(p), %mborobakRx(k,"p")=p
 }
 set m=%mborobakRx(k), m.Text=s"""

_TEXT = "VARCHAR(3641144)"
_PATTERN = "VARCHAR(32000)"

# name: (parameters, return type, body)
FUNCTIONS: dict[str, tuple[str, str, str]] = {
    # s RLIKE p: a match anywhere. $LOCATE is as fast as LIKE.
    "RegexpLike1": (f"s {_TEXT}, p {_PATTERN}", "INT", """
 quit:(s="")||(p="") ""
 set:s=$c(0) s="" set:p=$c(0) p="(?:)"
 quit $locate(s,p)>0"""),
    "RegexpReplace1": (f"s {_TEXT}, p {_PATTERN}, r {_TEXT}", _TEXT, """
 quit:r="" ""
 set:r=$c(0) r=""
""" + _MATCHER + """
 set out=m.ReplaceAll(r)
 quit $select(out="":$c(0),1:out)"""),
    # Spark: '' without a match or when the group took no part; an error for a group beyond the pattern's
    "RegexpExtract1": (f"s {_TEXT}, p {_PATTERN}, i INT", _TEXT, """
 quit:i="" ""
""" + _MATCHER + """
 if (i<0)||(i>m.GroupCount) { ztrap "MBGROUP" }
 quit:'m.Locate() $c(0)
 set g=m.Group(i)
 quit $select(g="":$c(0),1:g)"""),
    "RegexpCount1": (f"s {_TEXT}, p {_PATTERN}", "INT", _MATCHER + """
 set n=0
 while m.Locate() { set n=n+1 }
 quit n"""),
    "RegexpSubstr1": (f"s {_TEXT}, p {_PATTERN}", _TEXT, _MATCHER + """
 quit:'m.Locate() ""
 set g=m.Group(0)
 quit $select(g="":$c(0),1:g)"""),
    "RegexpInstr1": (f"s {_TEXT}, p {_PATTERN}", "INT", _MATCHER + """
 quit $select(m.Locate():m.Start,1:0)"""),
}


def call(name: str, *args: str) -> str:
    """The call, wrapped in a CASE: IRIS evaluates ``NOT (f(…) = 1)`` as true when a user function returns NULL
    (probed on 2026.2), so ``~rlike`` kept NULL rows. Inside a CASE the NULL is a NULL."""
    return f'CASE WHEN 1 = 0 THEN NULL ELSE "{SCHEMA}"."{name}"({", ".join(args)}) END'


def create_sql(name: str) -> str:
    params, returns, body = FUNCTIONS[name]
    return f'CREATE FUNCTION "{SCHEMA}"."{name}"({params}) RETURNS {returns} LANGUAGE OBJECTSCRIPT {{{body}\n}}'


def ensure(session: "MborobakSession", names: frozenset[str] | set[str], operation: str | None) -> None:
    """Create the functions a statement needs and this connection hasn't seen yet."""
    missing = set(names) - session._functions_ready
    if not missing or session._executor is None:
        return
    executor = session._executor
    sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ?"
    existing = {r[0] for r in executor.lookup(sql, [SCHEMA])}
    for name in sorted(missing - existing):
        try:
            executor.execute(create_sql(name), [], operation)
        except MborobakExecutionError as exc:
            if name in {r[0] for r in executor.lookup(sql, [SCHEMA])}:
                continue  # another session created it meanwhile
            raise MborobakExecutionError(
                f"regular expressions need the SQL function {SCHEMA}.{name}, which Mborobak creates on first use, "
                f"and creating it failed ({exc.message}). Ask an administrator to grant CREATE FUNCTION in this "
                "namespace, or to run session.installFunctions() once", operation=operation,
                sqlcode=getattr(exc, "sqlcode", None), iris_message=getattr(exc, "iris_message", None),
                sql=create_sql(name)) from None
    session._functions_ready |= missing
