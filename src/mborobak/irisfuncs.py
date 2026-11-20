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


_HASH_IN = """
 quit:s="" ""
 set:s=$c(0) s=""
 set u=$zconvert(s,"O","UTF8")"""
_HEX_OUT = """
 set o=""
 set i=0
 while i<$length(h) { set i=i+1, c=$zhex($ascii(h,i)), o=o_$select($length(c)=1:"0",1:"")_c }
 quit $zconvert(o,"L")"""

FUNCTIONS.update({
    # hashes of the UTF-8 bytes, as lowercase hex (Spark's md5, sha1, sha2) and CRC-32 (compiler spec §7.19)
    "Md5Hex1": (f"s {_TEXT}", "VARCHAR(32)", _HASH_IN + "\n set h=$system.Encryption.MD5Hash(u)" + _HEX_OUT),
    "Sha1Hex1": (f"s {_TEXT}", "VARCHAR(40)", _HASH_IN + "\n set h=$system.Encryption.SHA1Hash(u)" + _HEX_OUT),
    "Sha2Hex1": (f"s {_TEXT}, bits INT", "VARCHAR(128)",
                 _HASH_IN + "\n set:bits=0 bits=256\n set h=$system.Encryption.SHAHash(bits,u)" + _HEX_OUT),
    "Crc32_1": (f"s {_TEXT}", "BIGINT", _HASH_IN + "\n quit $zcrc(u,7)"),
    # edit distance in characters, two rows of the classic table
    "Levenshtein1": (f"a {_TEXT}, b {_TEXT}", "INT", """
 quit:(a="")||(b="") ""
 set:a=$c(0) a="" set:b=$c(0) b=""
 set n=$length(a), m=$length(b)
 quit:n=0 m
 quit:m=0 n
 set j=-1
 while j<m { set j=j+1, prev(j)=j }
 set i=0
 while i<n {
  set i=i+1, cur(0)=i, ca=$extract(a,i), j=0
  while j<m {
   set j=j+1, x=prev(j)+1, y=cur(j-1)+1, z=prev(j-1)+($extract(b,j)'=ca)
   set cur(j)=$select(x<y:$select(x<z:x,1:z),1:$select(y<z:y,1:z))
  }
  kill prev merge prev=cur kill cur
 }
 quit prev(m)"""),
    # Embedded Python where Python's libraries are exact: arbitrary-size integers, URLs, time zones. NULLs are
    # handled in SQL before the call; bodies avoid ? and :name, which the driver reads as parameters.
    "Conv1": ("s VARCHAR(200), f INT, t INT", "VARCHAR(200)", """
    digits = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    s = '' if s == chr(0) else s.strip()
    if s == '' or not (2 <= abs(f) <= 36 and 2 <= abs(t) <= 36):
        return None
    neg = s.startswith('-')
    body = s.lstrip('-').upper()
    v = 0
    for ch in body:
        d = digits.find(ch)
        if d < 0 or d >= abs(f):
            break
        v = v * abs(f) + d
    if v >= 2 ** 64:
        raise OverflowError('CONV_OVERFLOW')
    if neg:
        v = -v
    if t > 0:
        v = v % (2 ** 64)
    elif v >= 2 ** 63:
        v -= 2 ** 64
    sign = '-' if v < 0 else ''
    v = abs(v)
    out = ''
    while True:
        out = digits[v % abs(t)] + out
        v //= abs(t)
        if v == 0:
            break
    return sign + out
""", "PYTHON"),
    "ParseUrl1": ("u VARCHAR(32000), part VARCHAR(20), k VARCHAR(2000)", "VARCHAR(32000)", """
    import re
    import urllib.parse
    q = chr(63)
    u = '' if u == chr(0) else u
    k = '' if k == chr(0) else k
    allowed = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~/#[]@!$&()*+,;=%' + chr(39) + chr(58) + q)
    if any(ch not in allowed for ch in u):
        raise ValueError('INVALID_URL ' + u)
    sp = urllib.parse.urlsplit(u)
    opaque = bool(sp.scheme) and not u[len(sp.scheme) + 1:].startswith('/')
    authority = sp.netloc
    userinfo = authority.rpartition('@')[0] if '@' in authority else None
    hostport = authority.rpartition('@')[2]
    host = hostport.rpartition(chr(58))[0] if chr(58) in hostport and not hostport.endswith(']') else hostport
    path, query, ref = sp.path, sp.query, sp.fragment
    if opaque:
        path, query = None, ''
    if part == 'QUERY' and k != '':
        m = re.search('(&|^)' + re.escape(k) + '=([^&]*)', query or '')
        return m.group(2) if m else None
    values = dict(HOST=host or None, PATH=path, QUERY=query or None, REF=ref or None,
                  PROTOCOL=sp.scheme or None, FILE=None if path is None else path + (q + query if query else ''),
                  AUTHORITY=authority or None, USERINFO=userinfo)
    value = values.get(part)
    return chr(0) if value == '' else value
""", "PYTHON"),
    "UtcShift1": ("ts VARCHAR(40), tz VARCHAR(100), toutc INT", "VARCHAR(40)", """
    import datetime
    import zoneinfo
    name = tz.strip()
    upper = name.upper()
    if upper in ('UTC', 'GMT', 'Z', 'UT', 'UCT'):
        zone = datetime.timezone.utc
    else:
        offset = name
        for prefix in ('GMT', 'UTC', 'UT'):
            if upper.startswith(prefix) and len(name) > len(prefix) and name[len(prefix)] in '+-':
                offset = name[len(prefix):]
                break
        if offset[0] in '+-':
            sign = -1 if offset[0] == '-' else 1
            hours, sep, minutes = offset[1:].partition(chr(58))
            zone = datetime.timezone(sign * datetime.timedelta(hours=int(hours), minutes=int(minutes or 0)))
        else:
            zone = zoneinfo.ZoneInfo(name)
    value = datetime.datetime.fromisoformat(ts)
    utc = datetime.timezone.utc
    if toutc:
        out = value.replace(tzinfo=zone).astimezone(utc)
    else:
        out = value.replace(tzinfo=utc).astimezone(zone)
    return out.replace(tzinfo=None).isoformat(sep=' ')
""", "PYTHON"),
})


def call(name: str, *args: str) -> str:
    """The call, wrapped in a CASE: IRIS evaluates ``NOT (f(…) = 1)`` as true when a user function returns NULL
    (probed on 2026.2), so ``~rlike`` kept NULL rows. Inside a CASE the NULL is a NULL."""
    return f'CASE WHEN 1 = 0 THEN NULL ELSE "{SCHEMA}"."{name}"({", ".join(args)}) END'


def create_sql(name: str) -> str:
    params, returns, body, *language = FUNCTIONS[name]
    lang = language[0] if language else "OBJECTSCRIPT"
    return f'CREATE FUNCTION "{SCHEMA}"."{name}"({params}) RETURNS {returns} LANGUAGE {lang} {{{body}\n}}'


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
