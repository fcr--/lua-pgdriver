local pgdriver = require 'pgdriver'
local copasOk, copas = pcall(require, 'copas')

local examples = {}

-- A few examples:
--   pgdriver:new{unixPath='/var/local/run/postgresql/.s.PGSQL.<port>',connectparams.database='template1'}
--   pgdriver:new{host='1.2.3.4',connectparams.user='myuser',password='mypassword'}

local function handle_errors(fn)
  local _unpack = unpack or table.unpack
  local function handler(err)
    io.stderr:write(debug.traceback(tostring(err), 2), '\n')
    return err
  end
  return function(...)
    local res = {xpcall(fn, handler, ...)}
    if res[1] then return _unpack(res, 2) else error(res[2]) end
  end
end

local ssl = require'ssl'
local sslparams = {
  mode = 'client',
  protocol = 'tlsv1_2',
  key = os.getenv'HOME' .. '/.postgresql/postgresql.key',
  certificate = os.getenv'HOME' .. '/.postgresql/postgresql.crt',
  cafile = os.getenv'HOME' .. '/.postgresql/root.crt',
  verify = "peer",
  options = {"all", "no_sslv3"}
}

-- example for unix namespace sockets:
-- db=pgdriver:new{} --sync+unix
-- db=pgdriver:new{socketWrapper=copas.wrap} --copas+unix

-- examples for tcp:
-- db=pgdriver:new{host='localhost', password='foobar'} --sync+tcp
-- db=pgdriver:new{host='localhost', password='foobar', socketWrapper=copas.wrap} --copas+tcp

-- examples for ssl:
-- db=pgdriver:new{host='fideo',sslSocketWrapper=function(s)s=assert(ssl.wrap(s,sslparams));s:dohandshake()return s end} --sync+ssl
-- db=pgdriver:new{host='fideo', socketWrapper=copas.wrap,
--      sslSocketWrapper=function(s)s:dohandshake(sslparams)return s end} --copas+ssl

function examples.sync_load_test()
  local db = pgdriver:new{}
  print 'connected'
  for i = 1, 100000 do
    local nrows = 0
    for row in db:query('select 1+1 as foo union all select 3') do
      nrows = nrows + 1
      --print(i, row[1], row.foo)
      --print(i, #dbs, 'row:', tabletostring(row))
    end
    --print(i, nrows)
  end
end

function examples.mquery_sync_test()
  local db = pgdriver:new{}
  local nrow = 0
  for q in db:mquery('select coalesce($1, 0) + coalesce($2, 0) as s union all select -1;', {{1, 2}, {3}, {nil, 4}}) do
    for row in q do
      nrow = nrow + 1
      assert(tonumber(row.s) == ({3, -1, 3, -1, 4, -1})[nrow])
    end
  end
end

function examples.load_test()
  copas.limit = require 'copas.limit'
  copas.autoclose = false
  local q = copas.limit.new(10)

  copas.addthread(handle_errors(function()
    local dbs = {}
    for i = 1, 10 do
      q:addthread(handle_errors(function(i)
        dbs[i] = pgdriver:new{socketWrapper=copas.wrap}
      end), i)
    end
    q:wait()
    print 'connected'
    for i = 1, 100000 do
      q:addthread(handle_errors(function()
        local db = table.remove(dbs)
        local nrows = 0
        for row in db:query('select 1+1 as foo union all select 3') do
          nrows = nrows + 1
          --print(i, row[1], row.foo)
          --print(i, #dbs, 'row:', tabletostring(row))
        end
        print(i, nrows)
        table.insert(dbs, db)
      end))
    end
    q:wait()
  end))
  copas.loop()
end

return examples
