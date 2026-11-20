<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RedisDistributedMapCacheClientService

## Redis Enterprise Software and Redis Cloud

Configure a `RedisConnectionPoolService` and select it as the **Redis Connection Pool** for this service.

| Connection pool property | Value |
|---|---|
| **Redis Mode** | `Standalone` for a database endpoint that routes commands through the Redis proxy, including sharded databases. Select `Cluster` only if the endpoint exposes the Redis Cluster API and its advertised nodes are reachable from NiFi. |
| **Connection String** | The database endpoint as `hostname:port`. Use the database endpoint, not a management console or REST API endpoint. Do not include a `redis://` or `rediss://` scheme. |
| **Database Index** | `0`. Redis Enterprise Software and Redis Cloud identify databases by endpoint. |
| **Username** | The database ACL user, or `default` when using the default user. |
| **Password** | The database user's password, supplied through a sensitive parameter. |
| **SSL Context Service** | Configure this when the database endpoint requires TLS. Trust the database CA, and supply a client certificate and private key when mutual TLS is required. |

`Standalone` describes the client connection topology. It does not require an unsharded Redis server and does not
disable the availability or sharding provided by the Redis database endpoint. In `Standalone` mode, NiFi connects to
the database proxy, and the proxy routes each command to the shard that owns the key, across nodes when the database
spans several. See [proxy policies](https://redis.io/docs/latest/operate/rs/databases/configure/proxy-policy/) and
[Redis Cloud clustering](https://redis.io/docs/latest/operate/rc/databases/configuration/clustering/).

`Cluster` mode uses the [OSS Cluster API](https://redis.io/docs/latest/operate/rs/clusters/optimize/oss-cluster-api/):
the client discovers the hash slot topology, connects to the node that owns each key, and follows redirects. It can
reduce forwarding inside the database, but it is optional and is not required to use a sharded or multi-node database.

## Atomic Operations and Permissions

Conditional insert and replace operations act on one key at a time. The get-and-insert and compare-and-set operations
use `EVAL` with a single explicitly declared key, keeping the read, conditional write, and TTL update together on the
server. `WATCH`, `MULTI`, and `EXEC` are not required. Scripts and values are binary safe. Existing keys, values, and
raw-value revisions retain their format.

The Redis user needs permission for `EVAL` and the commands executed by these scripts (`GET` and `SET`), as well as
commands used by the cache operations the flow calls (including `EXISTS`, `DEL`, `MSET`, and `EXPIRE`). Configure key
permissions for the serialized cache keys. A server configured to prohibit scripting cannot use this atomic cache
service.

TTL is applied when an entry is inserted or successfully replaced. A failed comparison or an insert against an
existing key leaves the value and its expiration unchanged. A TTL of `0 secs` creates entries without expiration.

## Upgrading from Earlier Versions

Earlier versions implemented `getAndPutIfAbsent` and `replace` with `WATCH`, `MULTI`, and `EXEC`. Those operations
now run as Lua scripts. Before upgrading, grant the Redis user permission for `EVAL`, `GET`, and `SET` on the
serialized cache keys. A user that is allowed `WATCH`, `MULTI`, and `EXEC` but not `EVAL` fails these operations with
a `NOPERM` error, and the cache entry is left unchanged.

## Deployment Considerations

Single-key atomicity applies at the endpoint executing the operation. Redis Active-Active replication does not provide
a globally serialized compare-and-set across regions. Flows that require global coordination, such as duplicate
detection or Wait/Notify, should use one authoritative database rather than independent writable Active-Active
regions.

Bulk `putAll` retains its existing `MSET` behavior; it does not promise a transaction across multiple hash slots.
Follow the database's multi-key command restrictions and use a common hash tag when a deployment requires keys in one
slot.
