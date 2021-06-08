local copas = require 'copas'
local semaphore = require 'copas.semaphore'

local Pool = {}
Pool.__index = Pool

function Pool:new(opts)
  local max_resources = opts.max_resources or 10
  local res = setmetatable({
    sem = semaphore.new(max_resources, max_resources, opts.timeout),
    factory = opts.factory,  -- () => T
    closer = opts.closer or function(res) res:close() end,  -- T => ()
    available_resources = nil,  -- linked_list<T>
    last_available_resource = nil,  -- reference to last node
    expiration = opts.expiration,  -- nil or delta in seconds
    closed = false,
  }, self)
  return res
end

function Pool:take_available_resource()
  local node = self.available_resources
  if node then
    self.available_resources = node.next
    if not node.next then
      self.last_available_resource = nil
    end
    node.next = nil
  end
  return node
end

function Pool:with(fn)
  assert(not self.closed, 'Pool closed')
  assert(self.sem:take())
  local node = self:take_available_resource()

  -- close all expired resources
  while node and self.expiration and node.time + self.expiration < os.time() do
    pcall(self.closer, node.resource)
    node = self:take_available_resource()
  end

  if not node then
    -- try create a new one... it might fail
    print 'new instance'
    local ok, res = pcall(self.factory)
    if not ok then
      self.sem:give()
      error(res, 0)
    end
    node = {resource=res, next=nil}
  end

  node.time = os.time()

  return (function(ok, ...)
    if self.closed then
      pcall(self.closer, node.resource)
    else
      if self.last_available_resource then
        self.last_available_resource.next = node
      else
        self.available_resources = node
      end
      self.last_available_resource = node
    end
    self.sem:give()
    if not ok then
      error((...), 0)
    end
    return ...
  end)(pcall(fn, node.resource))
end

function Pool:close()
  self.closed = true
  local node = self:take_available_resource()
  while node do
    pcall(self.closer, node.resource)
    node = self:take_available_resource()
  end
end

return Pool
