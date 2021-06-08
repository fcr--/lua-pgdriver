local record pgdriver
    record Md5Library
        -- based on: https://github.com/kikito/md5.lua
        sumhexa: function(message: string): string
    end
    record ConnectionParams
        host: string  -- defaults to using unix domain sockets
        port: integer  -- defaults to 5432
        user: string  -- defaults to os.getenv 'USER' or os.getenv 'USERNAME'
        password: string  -- defaults to no password
        database: string  -- defaults to user
        unixPath: string  -- defaults to /var/run/postgresql/.s.PGSQL.<port>

        debug: boolean  -- if true prints all meessages, defaults to false
        md5: Md5Library  -- defaults to wishfully doing require 'md5' (needed for md5auth)

        -- Set to copas.wrap if you want to use async copas, otherwise the connections
        -- will be synchronous:
        socketWrapper: function(socket: FILE): FILE
        -- Function that does the SSL handshake, this is how you enable SSL:
        --   sync: function(s)s=assert(ssl.wrap(s,sslparams));s:dohandshake()return s end
        --  copas: function(s)s:dohandshake(sslparams)return s end
        sslSocketWrapper: function(socket: FILE): FILE
    end

    -- RowResult will be indexed both by column name and number (1-based):
    type RowResult = {(number | string): string}
    type RowIterator = function(): RowResult
    record Call
        {string|number|boolean|nil}
        maxrows: integer  -- defaults to retrieving all available rows
    end
    record Connection
        close: function(conn: Connection)
        query: function(conn: Connection, sql: string): RowIterator
        mquery: function(conn: Connection, sql: string, calls: {Call}): RowIterator
        mexec: function(conn: Connection, sql: string, calls: {Call}): {RowResult}
    end
    new: function(cls: pgdriver, options: ConnectionParams): Connection
end

return pgdriver
