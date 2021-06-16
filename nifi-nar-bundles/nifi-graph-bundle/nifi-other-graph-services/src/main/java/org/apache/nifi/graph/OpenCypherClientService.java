/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.graph;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencypher.gremlin.neo4j.driver.GremlinDatabase;

import java.util.HashMap;
import java.util.Map;

@CapabilityDescription("A client service that uses the OpenCypher implementation of the Cypher query language to connect to " +
        "databases other than Neo4J that are on the supported list of OpenCypher-compatible products. For more information, see: " +
        "http://www.opencypher.org/")
@Tags({ "cypher", "opencypher", "graph", "database", "janus" })
public class OpenCypherClientService extends AbstractTinkerpopClientService implements GraphClientService {
    private volatile Driver gremlinDriver;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        Cluster cluster = buildCluster(context);

        gremlinDriver = GremlinDatabase.driver(cluster);
    }

    @OnDisabled
    public void onDisabled() {
        gremlinDriver.close();
    }

    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

    private Map<String, Object> handleInternalNode(Map<String, Object> recordMap) {
        if (recordMap.size() == 1) {
            String key = recordMap.keySet().iterator().next();
            Object value = recordMap.get(key);
            if (value instanceof InternalNode) {
                return ((InternalNode)value).asMap();
            }
        }

        return recordMap;
    }

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        try (Session session = gremlinDriver.session()) {
            StatementResult result = session.run(query, parameters);
            long count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> asMap = handleInternalNode(record.asMap());
                handler.process(asMap, result.hasNext());
                count++;
            }

            Map<String,String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
            resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
            resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
            resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return transitUrl;
    }
}
