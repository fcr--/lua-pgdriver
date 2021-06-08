local record pool
  record PoolOptions<T>
    max_resources: integer
    factory: function(): T
    closer: function(resource: T)
    expiration: number  -- in seconds
  end
  record Pool<T>
    closed: boolean
    close: function(self: Pool<T>)
    with: function<R>(self: Pool<T>, fn: function(resource: T): R): R
  end

  new: function<T>(cls: pool, opts: PoolOptions): Pool<T>
end

return pool
