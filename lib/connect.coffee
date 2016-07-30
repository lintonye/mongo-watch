{Server, Db, ReplSetServers} = require 'mongodb'
_ = require 'lodash'

log = (msg) -> console.log "#{new Date()} - MongoWatch - #{msg}"

module.exports = ({db, host, port, dbOpts, serverOpts, dbListeners, username, password, authdb, replicaSet}, done) ->
  _.merge {native_parser: true}, dbOpts


  if replicaSet
    replSetServers = []
    _(replicaSet).forEach (replicaServer) ->
      replSetServers.push new Server(replicaServer.host, replicaServer.port)
      return

    connection = new ReplSetServers(replSetServers, serverOpts)
  else
    connection = new Server(host, port, serverOpts)

  client = new Db(db, connection, dbOpts)

  validEvents = ['authenticated', 'close', 'error', 'fullsetup', 'parseError', 'reconnect', 'timeout']
  for event, listener of dbListeners
    if (validEvents.indexOf(event) < 0)
      log "Invalid db event #{event}"
    else if listener?
      log "Adding listener: #{event}..."
      client.on(event, (e) -> listener(client, event, e))

  console.log "MongoWatch: connecting to #{host}:#{port}... serverOpts:#{JSON.stringify(serverOpts)}, dbOpts:#{JSON.stringify(dbOpts)}"

  client.open (err) ->
    return done(err) if err

    # authenticate if credentials were provided
    if username? or password?
      client.authenticate username, password, {authdb: authdb}, (err, result) ->
        done err, client

    else
      done err, client
