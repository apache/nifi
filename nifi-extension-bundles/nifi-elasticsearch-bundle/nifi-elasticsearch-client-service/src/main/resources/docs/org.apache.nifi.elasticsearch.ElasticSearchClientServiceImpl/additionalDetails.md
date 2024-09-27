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

# ElasticSearchClientServiceImpl

## Sniffing

The Elasticsearch Sniffer can be used to locate Elasticsearch Nodes within a Cluster to which you are connecting. This
can be beneficial if your cluster dynamically changes over time, e.g. new Nodes are added to maintain performance during
heavy load.

Sniffing can also be used to update the list of Hosts within the Cluster if a connection Failure is encountered during
operation. In order to "Sniff on Failure", you **must** also enable "Sniff Cluster Nodes".

Not all situations make sense to use Sniffing, for example if:

* Elasticsearch is situated behind a load balancer, which dynamically routes connections from NiFi
* Elasticsearch is on a different network to NiFi

There may also be need to set some of
the [Elasticsearch Networking Advanced Settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-network.html),
such as `network.publish_host` to ensure that the HTTP Hosts found by the Sniffer are accessible by NiFi. For example,
Elasticsearch may use a network internal `publish_host` that is inaccessible to NiFi, but instead should use an
address/IP that NiFi understands. It may also be necessary to add this same address to Elasticsearch's
`network.bind_host` list.

See [Elasticsearch sniffing best practices: What, when, why, how](https://www.elastic.co/blog/elasticsearch-sniffing-best-practices-what-when-why-how)
for more details of the best practices.

## Resources Usage Consideration

This Elasticsearch client relies on a `RestClient` using the Apache HTTP Async Client. By default, it will start one
dispatcher thread, and a number of worker threads used by the connection manager. There will be as many worker thread as
the number of locally detected processors/cores on the NiFi host. Consequently, it is highly recommended to have only
one instance of this controller service per remote Elasticsearch destination and have this controller service shared
across all the Elasticsearch processors of the NiFi flows. Having a very high number of instances could lead to resource
starvation and result in OOM errors.