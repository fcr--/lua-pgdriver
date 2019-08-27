pcall(require, 'host.init.__pkg') -- integration with ulua
pcall(require, 'luarocks.loader') -- try to use luarocks loader
local socket = require 'socket'
local unixOk, unix = pcall(require, 'socket.unix')
local copas = require 'copas'

local function class(super)
  local mt = setmetatable({}, super)
  mt.__index = mt
  if not mt.new then
    function mt:new(...)
      local obj = setmetatable({}, self)
      if obj._init then obj:_init(...) end
      return obj
    end
  end
  return mt
end

local pgdriver = class()

-- pgdriver.FE_MESSAGES = {} -- name:string -> message
pgdriver.MESSAGES = {} -- name:string -> message
pgdriver.MESSAGES_BY_CODE = {} -- id:char -> name:string -> message

--[[ pgdriver attributes:
  socketWrapper: function that takes a connectable tcp/unix socket, and returns the socket that will be used:
    for simple synchronous tcp connections just leave the default (nil)
    for ssl synchronous connections: function(skt)return ssl.wrap(skt, sslparams)end,
    for asynchronous tcp (inside copas): copas.wrap,
    for asynchronous ssl (inside copas): function(skt)return copas.wrap(skt, sslparams)end, ...
  host: hostname or ip (default localhost) to connect to
  port: port name or number (default 5432) to connect to
  connectparams: a table with the parameters to send to postgresql on connect
  connectparams.user: username to connect with (default 'USER' or 'USERNAME' environment variables)
  connectparams.database: database to use (default connectparams.user)
  password: string with password
  socket: connected socket or nil.
  md5: to be able to use md5auth we need require'md5' to work or this option to contain an object with md5.sumhexa
  unixPath: path to use with unix sockets, you can use the syntax <variable> to reference options like <port> or
    <connectparams.user>.  Defaults to '/var/run/postgresql/.s.PGSQL.<port>'
  status: 'Closed', 'ReadyForQuery', 'InQuery'
  currentQuery: object pointing to the current query
  commandComplete: the output of the last CommandComplete (showing affected rows for example)
  sm: state machine
]]

local function tabletostring(obj)
  if type(obj) == 'table' then
    local buffer = {}
    local maxi = 0
    for i, v in ipairs(obj) do
      buffer[#buffer+1], maxi = tabletostring(v), i
    end
    for k, v in pairs(obj) do
      if type(k)~='number' or k<1 or k>maxi or k~=math.floor(k) then
        if type(k) == 'string' and k:find '^[%a_][%w_]*$' then
          buffer[#buffer+1] = k .. '=' .. tabletostring(v)
        else
          buffer[#buffer+1] = '[' .. tabletostring(k) .. ']=' .. tabletostring(v)
        end
      end
    end
    return '{'..table.concat(buffer,',')..'}'
  elseif type(obj) == 'string' then
    return string.format('%q', obj)
  else
    return tostring(obj)
  end
end

local nullbyte = string.char(0)

local function encodeInt32(n)
  local bytes = {0, 0, 0, 0}
  for i = 1, 4 do
    bytes[i] = n % 256
    n = math.floor(n / 256)
  end
  return string.char(bytes[4], bytes[3], bytes[2], bytes[1])
end

local function encodeInt16(n)
  return string.char(math.floor(n/256)%256, n%256)
end

local function decodeInt32(str, offset)
  offset = offset or 1
  local b1, b2, b3, b4 = str:byte(offset, offset+3)
  local n = ((b1 * 256 + b2) * 256 + b3) * 256 + b4
  return (n > 0x7fffffff) and (n - 0x100000000) or n
end

local function decodeInt16(str, offset)
  offset = offset or 1
  local b1, b2 = str:byte(offset, offset + 1)
  local n = b1 * 256 + b2
  return (n > 0x7fff) and (n - 0x10000) or n
end

pgdriver.utils = {
  class = class,
  tabletostring = tabletostring,
  encodeInt16 = encodeInt16,
  encodeInt32 = encodeInt32,
  decodeInt16 = decodeInt16,
  decodeInt32 = decodeInt32
}

pgdriver.types = {}

-------------------------------------------------------------------------------

local AbstractField = class()
pgdriver.types.AbstractField = AbstractField

-- fluid setter
function AbstractField:with(options)
  for k, v in pairs(options) do
    self[k] = v
  end
  return self
end

-- this method may be called by the child classes to simplify the constructor code
function AbstractField:initprops(props, options)
  for k, v in pairs(options) do
    assert(props[k], 'option ' .. k .. ' not supported in type ' .. self.T)
  end
  for k, v in pairs(props) do
    local typesmap = {}
    for t in v:gmatch'%w+' do typesmap[t] = true end
    assert(typesmap[type(options[k])], 'option ' .. k .. ' does not have type ' .. v)
    self[k] = options[k]
  end
end

-- abreviate ":new" syntax, allowing object creation by just calling the class
function AbstractField:__call(...)
  return self:new(...)
end

function AbstractField:formatrec(buffer, i, value)
  error('Field ' .. self.name .. ':' .. self.T .. ' does not support formatting')
end

function AbstractField:parse(res, data, cursor)
  error('Field ' .. self.name .. ':' .. self.T .. ' does not support parsing')
end

-------------------------------------------------------------------------------

local Bytes = class(AbstractField):with{T='Bytes'}
pgdriver.types.Bytes = Bytes

function Bytes:_init(options)
  self:initprops({name='string', value='string|nil', length='number|nil'}, options)
  if self.value then assert(#self.value == self.length) end
end

function Bytes:format(buffer, i, value)
  buffer[i] = value[self.name] or self.value
  if type(buffer[i]) ~= 'string' then error('string expected on field ' .. self.name) end
  return i + 1
end

function Bytes:parse(res, data, cursor)
  local value
  if not self.length then
    value = data:sub(cursor)
  else
    value = data:sub(cursor, cursor + self.length - 1)
    if #value ~= self.length then return nil, 'incomplete field '..self.name end
  end
  if self.value and value ~= self.value then
    return nil, ('%q received, %q expected on field %s'):format(value, self.value, self.name)
  end
  res[self.name] = value
  return cursor + #value
end

-------------------------------------------------------------------------------

local Int16Array = class(AbstractField):with{T='Int16Array'}
pgdriver.types.Int16Array = Int16Array

function Int16Array:_init(options)
  self.children = {}
  for i, child in ipairs(options) do self.children[i] = child end
  self.initprops({name='string'}, options)
end

function Int16Array:format(buffer, i, value)
  local array = value[self.name]
  if type(array) ~= 'table' then error('array expected on field ' .. self.name) end
  buffer[i] = encodeInt16(#array)
  for j = 1, #array do
    buffer[i+j] = encodeInt16(array[j])
  end
  return i + #array + 1
end

function Int16Array:parse(res, data, cursor)
  local len = decodeInt16(data, cursor)
  local arr = {}
  for i = 1, len do
    arr[i] = decodeInt16(data, cursor + 2 * i)
  end
  res[self.name] = arr
  return cursor + 2 * (len + 1)
end

-------------------------------------------------------------------------------

local Int32Length = class(AbstractField):with{T='Int32Length'}
pgdriver.types.Int32Length = Int32Length

function Int32Length:_init(options)
  self:initprops({name='string'}, options)
end

function Int32Length:format(buffer, i, value)
  assert(buffer.lengthField == nil, 'only one length field is allowed')
  buffer.lengthField = i
  buffer[i] = '\0\0\0\4'
  return i + 1
end

function Int32Length:parse(res, data, cursor)
  res[self.name] = decodeInt32(data, cursor)
  if res[self.name] > #data - cursor + 1 then
    return nil, ('packet size from cursor=%d, length field=%d'):format(#data - cursor + 1, res[field.name])
  end
  return cursor + 4
end

-------------------------------------------------------------------------------

local String = class(AbstractField):with{T='String'}
pgdriver.types.String = String

function String:_init(options)
  self:initprops({name='string'}, options)
end

function String:format(buffer, i, value)
  value = value[self.name]
  if type(value) ~= 'string' then error('string expected on field ' .. self.name) end
  assert(not value:find'\0', 'null bytes not allowed in String fields')
  buffer[i] = value
  buffer[i+1] = nullbyte
  return i + 2
end

function String:parse(res, data, cursor)
  local nilpos = data:find('\0', cursor)
  if not nilpos then return nil, ('nil byte not found at %d on field %s'):format(cursor, self.name) end
  res[self.name] = data:sub(cursor, nilpos - 1)
  return nilpos + 1
end

-------------------------------------------------------------------------------

function pgdriver.registerMessages(messages)
  for name, msg in pairs(messages) do
    if msg[1].T == 'Bytes' and type(msg[1].value) == 'string' and #msg[1].value == 1 and msg[2].T == 'Int32Length' then
      local g = pgdriver.MESSAGES_BY_CODE[msg[1].value]
      if not g then
        g = {}
        pgdriver.MESSAGES_BY_CODE[msg[1].value] = g
      end
      g[name] = msg
    elseif msg[1].T ~= 'Int32Length' then -- message must at least have a length field
      error('Message ' .. name .. ' does not have a length field in a valid location' .. tabletostring(msg))
    end
    pgdriver.MESSAGES[name] = msg
  end
end

pgdriver.FE_MESSAGES = {
  Bind = {
    Bytes{name='id', value='B', length=1},
    Int32Length{name='length'},
    String{name='destination'},
    String{name='source'},
    {name='parameterFormats', T='Int16Array',
      {name='code', T='Int16'}},
    {name='parameterValues', T='Int16Array',
      {name='value', T='Int32Bytes'}},
    {name='resultFormats', T='Int16Array',
      {name='code', T='Int16'}}},
  CancelRequest = {
    Int32Length{name='length'},
    {name='code', T='Int32', value=1234*65536 + 5678},
    {name='pid', T='Int32'},
    {name='secret', T='Int32'}},
  Close = {
    Bytes{name='id', value='C', length=1},
    Int32Length{name='length'},
    Bytes{name='type', length=1}, -- 'S' (statement) or 'P' (portal)
    String{name='name'}},
  Describe = {
    Bytes{name='id', value='D', length=1},
    Int32Length{name='length'},
    Bytes{name='type', length=1}, -- 'S'=statement, 'P'=portal
    String{name='name'}},
  Execute = {
    Bytes{name='id', value='E', length=1},
    Int32Length{name='length'},
    String{name='portal'},
    {name='maxrows', T='Int32'}},
  Query = {
    Bytes{name='id', value='Q', length=1},
    Int32Length{name='length'},
    String{name='query'}}
}
-- Based on: https://www.postgresql.org/docs/9.3/protocol-message-formats.html
pgdriver.registerMessages {
  AuthenticationOk = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=0}},
  AuthenticationKerberosV5 = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=2}},
  AuthenticationCleartextPassword = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=3}},
  AuthenticationMD5Password = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=5},
    Bytes{name='salt', length=4}},
  AuthenticationSCMCredential = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=6}},
  AuthenticationGSS = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=7}},
  AuthenticationSSPI = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=9}},
  AuthenticationGSSContinue = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    {name='code', T='Int32', value=8},
    {name='authdata', T='Int32'}},
  BackendKeyData = {
    Bytes{name='id', value='K', length=1},
    Int32Length{name='length'},
    {name='pid', T='Int32'},
    {name='secret', T='Int32'}},
  BindComplete = {
    Bytes{name='id', value='1', length=1},
    Int32Length{name='length'}},
  CloseComplete = {
    Bytes{name='id', value='3', length=1},
    Int32Length{name='length'}},
  CommandComplete = {
    Bytes{name='id', value='C', length=1},
    Int32Length{name='length'},
    String{name='command'}},
  CopyData = {
    Bytes{name='id', value='d', length=1},
    Int32Length{name='length'},
    Bytes{name='data'}},
  DataRow = {
    Bytes{name='id', value='D', length=1},
    Int32Length{name='length'},
    {name='cols', T='Int16Array',
      {name='data', T='Int32Bytes'}}},
  -- ...
  EmptyQueryResponse = {
    Bytes{name='id', value='I', length=1},
    Int32Length{name='length'}},
  ErrorResponse = {
    Bytes{name='id', value='E', length=1},
    Int32Length{name='length'},
    {name='fields', T='NilTerminatedArray',
      Bytes{name='type', length=1},
      String{name='value'}}},
  Flush = {
    Bytes{name='id', value='H', length=1},
    Int32Length{name='length'}},
  FunctionCall = {
    Bytes{name='id', value='F', length=1},
    Int32Length{name='length'},
    {name='oid', T='Int32'},
    {name='formats', T='Int16Array',
      {name='code', T='Int16'}}, -- 0=text, 1=binary
    {name='values', T='Int16Array',
      {name='value', T='Int32Bytes'}},
    {name='resultCode', T='Int16'}}, -- 0=text, 1=binary
  FunctionCallResponse = {
    Bytes{name='id', value='V', length=1},
    Int32Length{name='length'},
    {name='response', T='Int32Bytes'}},
  NegotiateProtocolVersion = {
    Bytes{name='id', value='v', length=1},
    Int32Length{name='length'},
    {name='newestminor', T='Int32'},
    {name='notrecognized', T='Int32Array',
      String{name='name'}}},
  NoData = {
    Bytes{name='id', value='n', length=1},
    Int32Length{name='length'}},
  NoticeResponse = {
    Bytes{name='id', value='N', length=1},
    Int32Length{name='length'},
    {name='body', T='NilTerminatedArray',
      Bytes{name='type', length=1},
      String{name='value'}}},
  NotificationResponse = {
    Bytes{name='id', value='A', length=1},
    Int32Length{name='length'},
    {name='pid', T='Int32'},
    String{name='channel'},
    String{name='payload'}},
  ParameterDescription = {
    Bytes{name='id', value='t', length=1},
    Int32Length{name='length'},
    {name='parameters', T='Int16Array',
      {name='typeoid', T='Int32'}}},
  ParameterStatus = {
    Bytes{name='id', value='S', length=1},
    Int32Length{name='length'},
    String{name='name'},
    String{name='value'}},
  Parse = {
    Bytes{name='id', value='P', length=1},
    Int32Length{name='length'},
    String{name='name'},
    String{name='query'},
    {name='parameters', T='Int16Array',
      {name='typeoid', T='Int32'}}},
  ParseComplete = {
    Bytes{name='id', value='1', length=1},
    Int32Length{name='length'}},
  PasswordMessage = {
    Bytes{name='id', value='p', length=1},
    Int32Length{name='length'},
    String{name='password'}},
  PortalSuspended = {
    Bytes{name='id', value='s', length=1},
    Int32Length{name='length'}},
  ReadyForQuery = {
    Bytes{name='id', value='Z', length=1},
    Int32Length{name='length'},
    Bytes{name='status', length=1}}, -- 'I'=idle, 'T'=transaction block, 'E'=transaction failed
  RowDescription = {
    Bytes{name='id', value='T', length=1},
    Int32Length{name='length'},
    {name='cols', T='Int16Array',
      String{name='name'},
      {name='oid', T='Int32'},
      {name='colnum', T='Int16'},
      {name='typeoid', T='Int32'},
      {name='typelen', T='Int16'}, -- negative values mean variable size
      {name='atttypmod', T='Int32'},
      {name='format', T='Int16'}}}, -- 0=text, 1=binary
  SSLRequest = {
    Int32Length{name='length'},
    {name='code', T='Int32', value=1234*65536 + 5679}},
  StartupMessage = {
    Int32Length{name='length'},
    {name='version', T='Int32', value=3*65536 + 0},
    {name='parameters', T='StringMap', mandatory={'user'}}},
  Sync = {
    Bytes{name='id', value='S', length=1},
    Int32Length{name='length'}},
  Terminate = {
    Bytes{name='id', value='X', length=1},
    Int32Length{name='length'}}
}

-------------------------------------------------------------------------------

function pgdriver:_init(options)
  -- all parameters are deep copied
  self.socketWrapper = options.socketWrapper or function(skt) return skt end
  self.host = options.host or 'localhost'
  self.port = options.port or 5432
  self.connectparams = {}
  for k, v in pairs(options.connectparams or {}) do
    self.connectparams[k] = v
  end
  if not self.connectparams.user then
    self.connectparams.user = os.getenv 'USER' or os.getenv 'USERNAME' or error 'connectparams.user not specified'
  end
  self.connectparams.database = self.connectparams.database or self.connectparams.user
  self.md5 = options.md5
  self.password = options.password
  self.unixPath = (options.unixPath or '/var/run/postgresql/.s.PGSQL.<port>'):gsub('<[%w.]+>', function(path)
        local obj = self for component in path:gmatch'%w+' do obj = obj[component] end return obj
      end)
  self.sm = {}
  self:_connect()
end

function pgdriver:_connect()
  local name, pkt
  if self.socket then
    self.socket:close()
    self.socket = nil
  end
  self.status = 'Closed'
  if unixOk then
    self.socket = self.socketWrapper(unix())
    if not self.socket:connect(self.unixPath) then self.socket = nil end
  end
  if not self.socket then
    self.socket = self.socketWrapper(socket.tcp())
    assert(self.socket:connect(self.host, self.port))
  end
  -- print 'connected'
  self:_send(self.MESSAGES.StartupMessage, {parameters = self.connectparams})
  name, pkt = self:_receive()
  if name == 'AuthenticationMD5Password' then
    local md5 = self.md5 or require 'md5'
    local part = md5.sumhexa(self.password .. self.connectparams.user) .. pkt.salt
    self:_send(self.MESSAGES.PasswordMessage, {password='md5'..md5.sumhexa(part)})
  elseif name == 'AuthenticationOk' then
    -- nothing to do here.
  else
    error('packet not supported: ' .. name)
  end
  while true do
    local name, msg = self:_receive()
    -- print(name, tabletostring(msg))
    if name == 'ReadyForQuery' then
      self.status = 'ReadyForQuery'
      return self -- self is returned to allow a fluid interface
    elseif name == 'ErrorResponse' then
      self.socket:close()
      self.socket = nil
      error(tabletostring(msg))
    end
  end
end

function pgdriver:_receive(messageTypes)
  local headerlen = 5
  local header, body, len, msg
  if messageTypes then
    for _, msg in pairs(messageTypes) do
      if msg[1].T ~= 'Int32Length' then
        error('Messages specified in receive must start with length field. ' ..
              'Those with id are handled by default when messageTypes=nil.')
      end
    end
    headerlen = 4
  end
  header, msg = self.socket:receive(headerlen)
  if not header then -- ie: connection closed
    -- print(debug.traceback())
    self.socket:close()
    error('failure reading header from connection: ' .. msg)
  end
  len = decodeInt32(header:sub(headerlen - 3))
  assert(len >= 4, 'wrong length received')
  body, msg = self.socket:receive(len - 4)
  if not messageTypes then
    messageTypes = self.MESSAGES_BY_CODE[header:sub(1, 1)]
    assert(messageTypes, ('invalid message format %q'):format(header:sub(1, 1)))
  end
  local errors = {'Parsing failed:'}
  for name, msg in pairs(messageTypes) do
    local data = header .. body
    local pkt, err = self:_parse(msg, data)
    if pkt then
      if err ~= #data + 1 then print(('error? out of sync %s, %d!=%d+1, data=%q'):format(name, err, #data, data)) end
      return name, pkt
    end
    errors[#errors + 1] = ('  %q: %s'):format(name, err)
  end
  error(table.concat(errors, '\n'))
end

function pgdriver:_send(messageType, pkt)
  self.socket:send(pgdriver._format(messageType, pkt))
end

function pgdriver._format(messageType, value)
  local buffer = {}
  local i = 1
  for _, field in ipairs(messageType) do
    if field.format then
      i = field:format(buffer, i, value)
    elseif field.T == 'Int16' then
      local value = value[field.name] or field.value
      assert(type(value) == 'number', 'integer expected on field ' .. field.name)
      buffer[i] = encodeInt16(value)
      i = i + 1
    elseif field.T == 'Int32' then
      local value = value[field.name] or field.value
      assert(type(value) == 'number', 'integer expected on field ' .. field.name)
      buffer[i] = encodeInt32(value)
      i = i + 1
    elseif field.T == 'Int32Array' then
      local array = value[field.name]
      assert(type(array) == 'table', 'array expected on field ' .. field.name)
      local subbuffer = {encodeInt16(#array)}
      for j = 1, #array do
        subbuffer[j+1] = pgdriver._format(field, array[j])
      end
      buffer[i] = table.concat(subbuffer)
      i = i + 1
    elseif field.T == 'Int32Bytes' then
      local value = value[field.name] or field.value
      assert(value == nil or type(value) == 'string', 'string expected on field ' .. field.name)
      buffer[i] = value and encodeInt32(#value) .. value or encodeInt32(-1)
      i = i + 1
    elseif field.T == 'StringMap' then
      local value = value[field.name]
      assert(type(value) == 'table', 'table expected on field ' .. field.name)
      local subbuffer, j = {}, 0
      if field.mandatory then
        for _, k in ipairs(field.mandatory) do
          assert(type(value[k]) ~= nil, 'key ' .. k .. ' expected on field ' .. field.name)
        end
      end
      for k, v in pairs(value) do
        subbuffer[j + 1], subbuffer[j + 2] = tostring(k), nullbyte
        subbuffer[j + 3], subbuffer[j + 4] = tostring(v), nullbyte
        j = j + 4
      end
      subbuffer[j + 1] = nullbyte
      buffer[i] = table.concat(subbuffer)
      i = i + 1
    else
      error('unsupported field type '..field.T)
    end
  end
  if buffer.lengthField then
    local length = 4
    for i = buffer.lengthField + 1, #buffer do
      length = length + #buffer[i]
    end
    buffer[buffer.lengthField] = encodeInt32(length)
  end
  return table.concat(buffer)
end

function pgdriver:_parse(messageType, data)
  local cursor, res, msg = 1, {}
  for _, field in ipairs(messageType) do
    if field.parse then
      cursor, msg = field:parse(res, data, cursor)
      if not cursor then return nil, msg end
    elseif field.T == 'Int16' then
      if #data < cursor + 1 then return nil, 'incomplete field '..field.name end
      res[field.name] = decodeInt16(data:sub(cursor, cursor + 3))
      if field.value and res[field.name] ~= field.value then
        return nil, ('expected value %d, received %d'):format(field.value, res[field.name])
      end
      cursor = cursor + 2
    elseif field.T == 'Int32' then
      if #data < cursor + 3 then return nil, 'incomplete field '..field.name end
      res[field.name] = decodeInt32(data:sub(cursor, cursor + 3))
      if field.value and res[field.name] ~= field.value then
        return nil, ('expected value %d, received %d'):format(field.value, res[field.name])
      end
      cursor = cursor + 4
    elseif field.T == 'Int16Array' then
      local buffer = {}
      local count = decodeInt16(data:sub(cursor, cursor + 1))
      cursor = cursor + 2
      for i = 1, count do
        local arg1, arg2 = self:_parse(field, data:sub(cursor))
        if not arg1 then
          return nil, arg2
        else
          buffer[#buffer+1] = arg1
          cursor = cursor + arg2 - 1
        end
      end
      res[field.name] = buffer
    elseif field.T == 'Int32Bytes' then
      if #data < cursor + 3 then return nil, 'incomplete field '..field.name end
      local len = decodeInt32(data:sub(cursor, cursor + 3))
      cursor = cursor + 4
      if len >= 0 then
        res[field.name] = data:sub(cursor, cursor + len - 1)
      end
      if field.value and res[field.name] ~= field.value then
        return nil, ('expected value %d, received %d'):format(field.value, res[field.name])
      end
      cursor = cursor + math.max(0, len)
    elseif field.T == 'NilTerminatedArray' then
      local buffer = {}
      while (data:sub(cursor, cursor) or '\0') ~= '\0' do
        local arg1, arg2 = self:_parse(field, data:sub(cursor))
        if not arg1 then
          return nil, arg2
        else
          buffer[#buffer+1] = arg1
          cursor = cursor + arg2 - 1
        end
      end
      res[field.name] = buffer
      if cursor > #data then return nil, ('unexpected EOP at %d on field %s'):format(cursor, field.name) end
      cursor = cursor + 1
    else
      error('unsupported field type '..field.T)
    end
  end
  return res, cursor
end

-- consumes a single message blocking, then advancing the current state of the state machine
function pgdriver:step()
end

function pgdriver:query(sql)
  assert(type(sql) == 'string')
  assert(self.status == 'ReadyForQuery')
  self.status = 'InQuery'
  self:_send(self.FE_MESSAGES.Query, {query=sql})
  local currentQuery = {}
  self.currentQuery = currentQuery
  local cols
  local currentError
  return function()
    while true do
      if self.currentQuery ~= currentQuery then error 'Query cancelled' end
      local name, msg = self:_receive()
      -- print('debug: received', name, tabletostring(msg))
      if name == 'DataRow' then
        local row = {}
        for i, col in ipairs(msg.cols) do
          row[i] = col.data
          if cols and cols[i] and cols[i].name then
            row[cols[i].name] = col.data
          end
        end
        return row
      elseif name == 'RowDescription' then
        cols = msg.cols
      elseif name == 'ReadyForQuery' then
        self.currentQuery = nil
        self.status = 'ReadyForQuery'
        -- we believe that postgresql sends ReadyForQuery after ErrorResponse
        if currentError then error(currentError) end
        return
      elseif name == 'CommandComplete' then
        self.commandComplete = msg.command
      elseif name == 'ErrorResponse' then
        currentError = tabletostring(msg)
        -- docs say ErrorResponse is always followed by ReadyForQuery
      else
        error('unsupported message ' .. name)
      end
    end
  end
end

-- A few examples:
--   pgdriver:new{unixPath='/var/local/run/postgresql/.s.PGSQL.<port>',connectparams.database='template1'}
--   pgdriver:new{host='1.2.3.4',connectparams.user='myuser',password='mypassword'}

copas.limit = require 'copas.limit'
copas.autoclose = false
local q = copas.limit.new(10)

copas.addthread(function()
  local dbs = {}
  for i = 1, 10 do
    q:addthread(function(i)
      dbs[i] = pgdriver:new{socketWrapper=copas.wrap}
    end, i)
  end
  q:wait()
  print 'connected'
  for i = 1, 100000 do
    q:addthread(function()
      local db = table.remove(dbs)
      local nrows = 0
      for row in db:query('select 1+1 as pelota union select 3') do
        nrows = nrows + 1
        --print(i, row.pelota)
        --print(i, #dbs, 'row:', tabletostring(row))
      end
      print(i, nrows)
      table.insert(dbs, db)
    end)
  end
  q:wait()
end)
copas.loop()

return pgdriver
