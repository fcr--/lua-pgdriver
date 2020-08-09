pcall(require, 'host.init.__pkg') -- integration with ulua
pcall(require, 'luarocks.loader') -- try to use luarocks loader

local socket = require 'socket'
local unixOk, unix = pcall(require, 'socket.unix')

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
    for asynchronous (for both tcp and ssl) (inside copas): copas.wrap,
  sslSocketWrapper: function that takes a connected tcp socket, does ssl handshake and returns the socket that will be used:
    for ssl synchronous connections: function(s)s=assert(ssl.wrap(s,sslparams));s:dohandshake()return s end,
    for ssl over copas: function(s)s:dohandshake(sslparams)return s end
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
  local maxn = 0
  for k, v in pairs(options) do
    if type(k) == 'number' and props['children'] then
      maxn = math.max(maxn, k)
    else
      assert(props[k], 'option ' .. k .. ' not supported in type ' .. self.T)
    end
  end
  for k, v in pairs(props) do
    local typesmap = {}
    for t in v:gmatch'%w+' do typesmap[t] = true end
    if k == 'children' then
      self.children = {}
      for i = 1, #options do
        assert(typesmap[type(options[i])], 'option ' .. i .. ' does not have type ' .. v)
        self.children[i] = options[i]
      end
      assert(maxn == #options, 'disperse array option')
    else
      assert(typesmap[type(options[k])], 'option ' .. k .. ' does not have type ' .. v)
      self[k] = options[k]
    end
  end
end

-- abreviate ":new" syntax, allowing object creation by just calling the class
function AbstractField:__call(...)
  return self:new(...)
end

function AbstractField:format(buffer, i, value)
  error('Field ' .. self.name .. ':' .. self.T .. ' does not support formatting')
end

-- messageType: (forall T. T <: AbstractField)[], or just an array of AbstractField descendant instances
-- value: record to encode
function AbstractField.formatrec(messageType, value)
  local buffer = {}
  local i = 1
  for _, field in ipairs(messageType) do
    i = field:format(buffer, i, value)
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

function AbstractField:parse(res, data, cursor)
  error('Field ' .. self.name .. ':' .. self.T .. ' does not support parsing')
end

-- return: nil, errormessage
--     or: res, cursor
function AbstractField.parserec(messageType, data, cursor)
  local res, msg = {}
  cursor = cursor or 1
  for _, field in ipairs(messageType) do
    if not field.parse then
      error('field ' .. field.name .. ' typed ' .. field.T .. ' does not support parse')
    end
    cursor, msg = field:parse(res, data, cursor)
    if not cursor then return nil, msg end
  end
  return res, cursor
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

local Int16 = class(AbstractField):with{T='Int16'}
pgdriver.types.Int16 = Int16

function Int16:_init(options)
  self:initprops({name='string', value='number|nil'}, options)
end

function Int16:format(buffer, i, value)
  value = value[self.name] or self.value
  if type(value) ~= 'number' then error('number expected on field ' .. self.name) end
  buffer[i] = encodeInt16(value)
  return i + 1
end

function Int16:parse(res, data, cursor)
  local value = decodeInt16(data, cursor)
  if self.value and value ~= self.value then
    return nil, ('%q received, %q expected on field %s'):format(value, self.value, self.name)
  end
  res[self.name] = value
  return cursor + 2
end

-------------------------------------------------------------------------------

local Int16Array = class(AbstractField):with{T='Int16Array'}
pgdriver.types.Int16Array = Int16Array

function Int16Array:_init(options)
  self:initprops({name='string', children='table'}, options)
end

function Int16Array:format(buffer, i, value)
  local array = value[self.name]
  if type(array) ~= 'table' then error('array expected on field ' .. self.name) end
  buffer[i] = encodeInt16(#array)
  for j = 1, #array do
    buffer[i+j] = AbstractField.formatrec(self.children, array[j])
  end
  return i + #array + 1
end

function Int16Array:parse(res, data, cursor)
  local array = {}
  local count = decodeInt16(data, cursor)
  cursor = cursor + 2
  for i = 1, count do
    local arg1, arg2 = AbstractField.parserec(self.children, data, cursor)
    if not arg1 then
      return nil, arg2 -- nil, error string
    else
      array[i] = arg1
      cursor = arg2
    end
  end
  res[self.name] = array
  return cursor
end

-------------------------------------------------------------------------------

local Int32 = class(AbstractField):with{T='String'}
pgdriver.types.Int32 = Int32

function Int32:_init(options)
  self:initprops({name='string', value='number|nil'}, options)
end

function Int32:format(buffer, i, value)
  value = value[self.name] or self.value
  if type(value) ~= 'number' then error('number expected on field ' .. self.name) end
  buffer[i] = encodeInt32(value)
  return i + 1
end

function Int32:parse(res, data, cursor)
  local value = decodeInt32(data, cursor)
  if self.value and value ~= self.value then
    return nil, ('%q received, %q expected on field %s'):format(value, self.value, self.name)
  end
  res[self.name] = value
  return cursor + 4
end

-------------------------------------------------------------------------------

local Int32Array = class(AbstractField):with{T='Int32Array'}
pgdriver.types.Int32Array = Int32Array

function Int32Array:_init(options)
  self:initprops({name='string', children='table'}, options)
end

function Int32Array:format(buffer, i, value)
  local array = value[self.name]
  if type(array) ~= 'table' then error('array expected on field ' .. self.name) end
  buffer[i] = encodeInt32(#array)
  for j = 1, #array do
    buffer[i+j] = AbstractField.formatrec(self.children, array[j])
  end
  return i + #array + 1
end


-------------------------------------------------------------------------------

local Int32Bytes = class(AbstractField):with{T='String'}
pgdriver.types.Int32Bytes = Int32Bytes

function Int32Bytes:_init(options)
  self:initprops({name='string', value='string|nil'}, options)
end

function Int32Bytes:format(buffer, i, value)
  value = value[self.name] or self.value
  if value == nil then
    buffer[i] = encodeInt32(-1)
    return i + 1
  end
  if type(value) ~= 'string' then error('string expected on field ' .. self.name) end
  buffer[i] = encodeInt32(#value)
  buffer[i+1] = value
  return i + 2
end

function Int32Bytes:parse(res, data, cursor)
  local len = decodeInt32(data:sub(cursor, cursor + 3))
  cursor = cursor + 4
  -- len < 0 indicates a null parameter value
  if len >= 0 then
    res[self.name] = data:sub(cursor, cursor + len - 1)
  end
  if self.value and res[self.name] ~= self.value then
    return nil, ('expected value %d, received %d'):format(self.value, res[self.name])
  end
  return cursor + math.max(0, len)
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
    return nil, ('packet size from cursor=%d, length field=%d'):format(#data - cursor + 1, res[self.name])
  end
  return cursor + 4
end

-------------------------------------------------------------------------------

local NilTerminatedArray = class(AbstractField):with{T='NilTerminatedArray'}
pgdriver.types.NilTerminatedArray = NilTerminatedArray

function NilTerminatedArray:_init(options)
  self:initprops({name='string', children='table'}, options)
end

function NilTerminatedArray:parse(res, data, cursor)
  local buffer = {}
  while (data:byte(cursor) or 0) ~= 0 do
    local arg1, arg2 = AbstractField.parserec(self.children, data, cursor)
    if not arg1 then
      return nil, arg2
    end
    buffer[#buffer+1] = arg1
    cursor = arg2
  end
  res[self.name] = buffer
  if cursor > #data then return nil, ('unexpected EOP at %d on field %s'):format(cursor, self.name) end
  return cursor + 1
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

local StringMap = class(AbstractField):with{T='StringMap'}
pgdriver.types.StringMap = StringMap

function StringMap:_init(options)
  self:initprops({name='string', mandatory='table|nil'}, options)
end

function StringMap:format(buffer, i, value)
  value = value[self.name]
  assert(type(value) == 'table', 'table expected on field ' .. self.name)
  if self.mandatory then
    for _, k in ipairs(self.mandatory) do
      assert(type(value[k]) ~= 'nil', 'key ' .. k .. ' expected on field ' .. self.name)
    end
  end
  for k, v in pairs(value) do
    buffer[i], buffer[i + 1] = tostring(k), nullbyte
    buffer[i + 2], buffer[i + 3] = tostring(v), nullbyte
    i = i + 4
  end
  buffer[i] = nullbyte
  return i + 1
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

-- Based on: https://www.postgresql.org/docs/9.3/protocol-message-formats.html
-- list of messages only sent by pgdriver:
pgdriver.FE_MESSAGES = {
  Bind = {
    Bytes{name='id', value='B', length=1},
    Int32Length{name='length'},
    String{name='portal'},
    String{name='source'},
    Int16Array{name='parameterFormats',
      Int16{name='code'}},
    Int16Array{name='parameterValues',
      Int32Bytes{name='value'}},
    Int16Array{name='resultFormats',
      Int16{name='code'}}},
  CancelRequest = {
    Int32Length{name='length'},
    Int32{name='code', value=1234*65536 + 5678},
    Int32{name='pid'},
    Int32{name='secret'}},
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
    Int32{name='maxrows'}},
  Flush = {
    Bytes{name='id', value='H', length=1},
    Int32Length{name='length'}},
  Parse = {
    Bytes{name='id', value='P', length=1},
    Int32Length{name='length'},
    String{name='name'},
    String{name='query'},
    Int16Array{name='parameters',
      Int32{name='typeoid'}}},
  Query = {
    Bytes{name='id', value='Q', length=1},
    Int32Length{name='length'},
    String{name='query'}},
  Sync = {
    Bytes{name='id', value='S', length=1},
    Int32Length{name='length'}},
  Terminate = {
    Bytes{name='id', value='X', length=1},
    Int32Length{name='length'}},
}

-- Messages that can be received from the server, or (F&B) when they can be
-- sent by the driver as well.  They will be registered in pgdriver.MESSAGES.
-- Also, those with a Bytes{name='id', value=?, length=1} in their first field
-- will be registered into pgdriver.MESSAGES_BY_CODE.
pgdriver.registerMessages {
  AuthenticationOk = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=0}},
  AuthenticationKerberosV5 = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=2}},
  AuthenticationCleartextPassword = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=3}},
  AuthenticationMD5Password = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=5},
    Bytes{name='salt', length=4}},
  AuthenticationSCMCredential = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=6}},
  AuthenticationGSS = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=7}},
  AuthenticationSSPI = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=9}},
  AuthenticationGSSContinue = {
    Bytes{name='id', value='R', length=1},
    Int32Length{name='length'},
    Int32{name='code', value=8},
    Int32{name='authdata'}},
  BackendKeyData = {
    Bytes{name='id', value='K', length=1},
    Int32Length{name='length'},
    Int32{name='pid'},
    Int32{name='secret'}},
  BindComplete = {
    Bytes{name='id', value='2', length=1},
    Int32Length{name='length'}},
  CloseComplete = {
    Bytes{name='id', value='3', length=1},
    Int32Length{name='length'}},
  CommandComplete = {
    Bytes{name='id', value='C', length=1},
    Int32Length{name='length'},
    String{name='command'}},
  CopyData = { -- (F&B)
    Bytes{name='id', value='d', length=1},
    Int32Length{name='length'},
    Bytes{name='data'}},
  CopyDone = { -- (F&B)
    Bytes{name='id', value='c', length=1},
    Int32Length{name='length'}},
  DataRow = {
    Bytes{name='id', value='D', length=1},
    Int32Length{name='length'},
    Int16Array{name='cols',
      Int32Bytes{name='data'}}},
  -- ...
  EmptyQueryResponse = {
    Bytes{name='id', value='I', length=1},
    Int32Length{name='length'}},
  ErrorResponse = {
    Bytes{name='id', value='E', length=1},
    Int32Length{name='length'},
    NilTerminatedArray{name='fields',
      Bytes{name='type', length=1},
      String{name='value'}}},
  FunctionCall = {
    Bytes{name='id', value='F', length=1},
    Int32Length{name='length'},
    Int32{name='oid'},
    Int16Array{name='formats',
      Int16{name='code'}}, -- 0=text, 1=binary
    Int16Array{name='values',
      Int32Bytes{name='value'}},
    Int16{name='resultCode'}}, -- 0=text, 1=binary
  FunctionCallResponse = {
    Bytes{name='id', value='V', length=1},
    Int32Length{name='length'},
    Int32Bytes{name='response'}},
  NegotiateProtocolVersion = {
    Bytes{name='id', value='v', length=1},
    Int32Length{name='length'},
    Int32{name='newestminor'},
    Int32Array{name='notrecognized',
      String{name='name'}}},
  NoData = {
    Bytes{name='id', value='n', length=1},
    Int32Length{name='length'}},
  NoticeResponse = {
    Bytes{name='id', value='N', length=1},
    Int32Length{name='length'},
    NilTerminatedArray{name='body',
      Bytes{name='type', length=1},
      String{name='value'}}},
  NotificationResponse = {
    Bytes{name='id', value='A', length=1},
    Int32Length{name='length'},
    Int32{name='pid'},
    String{name='channel'},
    String{name='payload'}},
  ParameterDescription = {
    Bytes{name='id', value='t', length=1},
    Int32Length{name='length'},
    Int16Array{name='parameters',
      Int32{name='typeoid'}}},
  ParameterStatus = {
    Bytes{name='id', value='S', length=1},
    Int32Length{name='length'},
    String{name='name'},
    String{name='value'}},
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
    Int16Array{name='cols',
      String{name='name'},
      Int32{name='oid'},
      Int16{name='colnum'},
      Int32{name='typeoid'},
      Int16{name='typelen'}, -- negative values mean variable size
      Int32{name='atttypmod'},
      Int16{name='format'}}}, -- 0=text, 1=binary
  SSLRequest = {
    Int32Length{name='length'},
    Int32{name='code', value=1234*65536 + 5679}},
  StartupMessage = {
    Int32Length{name='length'},
    Int32{name='version', value=3*65536 + 0},
    StringMap{name='parameters', mandatory={'user'}}},
}

-------------------------------------------------------------------------------

function pgdriver:_init(options)
  -- all parameters are deep copied
  self.socketWrapper = options.socketWrapper or function(skt) return skt end
  self.sslSocketWrapper = options.sslSocketWrapper
  self.host = options.host
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
  if unixOk and not self.host then
    self.socket = assert(self.socketWrapper(assert(unix())))
    if not self.socket:connect(self.unixPath) then self.socket = nil end
  end
  if not self.socket then
    self.socket = assert(self.socketWrapper(assert(socket.tcp())))
    assert(self.socket:connect(self.host or 'localhost', self.port))
  end
  -- print 'connected'
  if self.sslSocketWrapper then
    self:_send(self.MESSAGES.SSLRequest, {})
    local S, msg = self.socket:receive(1)
    if S == 'S' then -- ie: connection closed
      self.socket = assert(self.sslSocketWrapper(self.socket))
    else
      pcall(self.socket.close, self.socket)
      self.socket = nil
      error(S and ('The server rejected the SSL request ('..S..')')
              or ('failure reading ssl response from connection: ' .. msg))
    end
  end
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

-- return: messagename, res
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
  len = decodeInt32(header, headerlen - 3)
  assert(len >= 4, 'wrong length received')
  body, msg = self.socket:receive(len - 4)
  if not messageTypes then
    messageTypes = self.MESSAGES_BY_CODE[header:sub(1, 1)]
    if not messageTypes then
      assert(false, ('invalid message format %q'):format(header:sub(1, 1)))
    end
  end
  local errors = {'Parsing failed:'}
  for name, msg in pairs(messageTypes) do
    local data = header .. body
    local pkt, arg2 = AbstractField.parserec(msg, data)
    if pkt then -- arg2 is the resulting cursor
      if arg2 ~= #data + 1 then
        print(('error? out of sync %s, %d!=%d+1, data=%q'):format(name, arg2, #data, data))
      end
      return name, pkt
    else -- arg2 is an error value
      errors[#errors + 1] = ('  %q: %s'):format(name, arg2)
    end
  end
  error(table.concat(errors, '\n'))
end

function pgdriver:_send(messageType, pkt)
  self.socket:send(AbstractField.formatrec(messageType, pkt))
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
  self.commandComplete = nil
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

-- sql: string
-- calls: array<{maxrows:nil|number} & array<nil|tostringable>>, the sql query will be
--     executed once for each entry in calls array.
function pgdriver:mquery(sql, calls)
  local name, pkt, currentError, cols, numparams
  assert(type(sql) == 'string')
  assert(self.status == 'ReadyForQuery')
  self.status = 'InQuery'

  -- Protocol summary:
  -- 1. send Parse, receive ParseComplete | ErrorResponse.
  -- 2. send Describe & Flush, receive ParameterDescription, receive RowDescription | NoData.
  -- For each call in calls:
  --   3.1. send Bind, receive BindComplete | ErrorResponse.
  --   3.2. send Execute, receive one or more DataRow, receive exactly one of:
  --        a. CommandComplete: the query does not return a resultset.
  --        b. EmptyQueryResponse: empty resultset.
  --        c. ErrorResponse: something failed in the middle (further cals get ignored).
  --        d. PortalSuspended: max rows reached.
  -- 4. send Sync, receive ReadyForQuery.

  self:_send(self.FE_MESSAGES.Parse, {name='', query=sql, parameters={}})
  self:_send(self.FE_MESSAGES.Describe, {name='', type='S'})
  self:_send(self.FE_MESSAGES.Flush, {})
  while true do
    name, pkt = self:_receive()
    -- print('debug: received', name, tabletostring(pkt))
    if name == 'ParseComplete' then
      -- ok
    elseif name == 'NoData' then
      break
    elseif name == 'ParameterDescription' then
      numparams = #pkt.parameters
    elseif name == 'RowDescription' then
      cols = pkt.cols
      break
    elseif name == 'ErrorResponse' then
      currentError = tabletostring(pkt)
      self:_send(self.FE_MESSAGES.Sync, {})
    elseif name == 'ReadyForQuery' then
      self.status = 'ReadyForQuery'
      error(currentError or 'unexpected ReadyForQuery without ErrorResponse')
    else
      error('unsupported message ' .. name)
    end
  end

  local values = {}
  for _, call in ipairs(calls) do
    for i = 1, numparams do
      values[i] = {value = call[i]~=nil and tostring(call[i]) or nil}
    end
    self:_send(self.FE_MESSAGES.Bind, {
      portal='', source='', -- unnamed statement
      parameterFormats={}, -- all parameters have text format
      parameterValues=values, -- array of {value:string|nil}
      resultFormats={},
    }) -- all results have text format
    self:_send(self.FE_MESSAGES.Execute, {portal='', maxrows=row.maxrows or 0})
  end
  self:_send(self.FE_MESSAGES.Sync, {})

  local finalized = false -- set to true when ReadyForQuery has been received
  return function()
    if finalized then return end
    local completed = false -- set to true when the specific execution has completed
    self.commandComplete = nil
    repeat
      name, pkt = self:_receive()
      if name == 'ReadyForQuery' then
        finalized = true
        return
      end
    until name ~= 'BindComplete'
    return function()
      while true do
        if completed or finalized then return end
        if name == nil then name, pkt = self:_receive() end
        -- print('debug: received', name, tabletostring(pkt))
        if name == 'DataRow' then
          name = nil
          local row = {}
          for i, col in ipairs(pkt.cols) do
            row[i] = col.data
            if cols and cols[i] and cols[i].name then
              row[cols[i].name] = col.data
            end
          end
          return row
        elseif name == 'ReadyForQuery' then
          finalized = true
          if currentError then error(currentError) end
          return
        elseif name == 'CommandComplete' then
          self.commandComplete = pkt.command
          completed = true
          return
        elseif name == 'EmptyQueryResponse' then
          completed = true
          return
        elseif name == 'ErrorResponse' then
          name = nil
          currentError = currentError or tabletostring(pkt) -- followed by ReadyForQuery
        elseif name == 'PortalSuspended' then
          completed = true
          return
        else
          error('unsupported message ' .. name)
        end
      end
    end
  end
end

return pgdriver
