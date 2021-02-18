# lua-pgdriver
Native sync/async PostgreSQL client written in lua.

# Warning.
This library is very incomplete.  It could even be said that it's in fact nothing more than a proof of code.

# Usage:
## Connecting (sync):

If unix namespace sockets are supported in your environment, then you can use (with `unixPath`, `port`, and `connectparams.database` as relevant options):
```lua
local conn = pgdriver:new{}
```

If you want a plain TCP connection, you can use (with only `host` as a mandatory option):
```lua
local conn = pgdriver:new{host='localhost', port=5432, password='foobar',
                          connectparams={user='myuser', database='myuser'}}
```

For SSL you need to pass a socket creator wrapper.  Same as a plain TCP connection but with an `sslSocketWrapper`:
```lua
-- your sslparams will probably look something like this:
local sslparams = {
  mode = 'client',
  protocol = 'tlsv1_2',
  key = os.getenv'HOME' .. '/.postgresql/postgresql.key',
  certificate = os.getenv'HOME' .. '/.postgresql/postgresql.crt',
  cafile = os.getenv'HOME' .. '/.postgresql/root.crt',
  verify = "peer",
  options = {"all", "no_sslv3"}
}

local conn = pgdriver:new{host='localhost', port=5432, password='foobar',
                          connectparams={user='myuser', database='myuser'},
                          sslSocketWrapper=function(s)s=assert(ssl.wrap(s,sslparams));s:dohandshake()return s end}
```
## Connecting (async):

Using unix namespace sockets:
```lua
local conn = pgdriver:new{socketWrapper=copas.wrap}
```

Using plain TCP connections:
```lua
local conn = pgdriver:new{host='localhost', port=5432, password='foobar',
                          connectparams={user='myuser', database='myuser'},
                          socketWrapper=copas.wrap}
```
Using SSL (here's a different beast):
```lua
local sslparams = { -- same as without copas
  mode = 'client',
  protocol = 'tlsv1_2',
  key = os.getenv'HOME' .. '/.postgresql/postgresql.key',
  certificate = os.getenv'HOME' .. '/.postgresql/postgresql.crt',
  cafile = os.getenv'HOME' .. '/.postgresql/root.crt',
  verify = "peer",
  options = {"all", "no_sslv3"}
}

local conn = pgdriver:new{host='localhost', port=5432, password='foobar',
                          connectparams={user='myuser', database='myuser'},
                          socketWrapper=copas.wrap
                          sslSocketWrapper=function(s)s:dohandshake(sslparams)return s end}
```

## Making queries:

Just a simple query with:
```lua
for row in conn:query [[SELECT 'uno' AS f UNION ALL SELECT 'dos']] do
  print(row[1], row.f) -- fields can be accesed both by name and by index
end
-- prints:
-- uno     uno
-- dos     dos
```

Or if your query has parameters (to avoid SQL injection), or if you want to execute the same statement many times:
```lua
for q in conn:mquery([[SELECT $1, i FROM generate_series(1, $2) i]], {
  {"a", 1},
  {"b", 2},
}) do
  for row in q do print(row[1], row[2]) end
end
-- prints:
a       1
b       1
b       2
```

If you don't care about the results or only want to collect them into an array, you can use the `mexec` method, that accepts the same arguments as `mquery` (in fact it's just a simple wrapper):
```lua
conn:mexec([[INSERT INTO foo(x, y) VALUES($1, $2)]], {{x1, y1}, {x2, y2}, {x3, y3}})
```

## Running a load test:
```bash
time luajit -e 'require"pgdriver".load_test()' | tail
```
