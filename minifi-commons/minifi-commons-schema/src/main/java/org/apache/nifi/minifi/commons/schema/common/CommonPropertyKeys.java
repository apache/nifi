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

package org.apache.nifi.minifi.commons.schema.common;

public class CommonPropertyKeys {
    public static final String CORE_PROPS_KEY = "Core Properties";
    public static final String FLOWFILE_REPO_KEY = "FlowFile Repository";
    public static final String SWAP_PROPS_KEY = "Swap";
    public static final String FLOW_CONTROLLER_PROPS_KEY = "Flow Controller";
    public static final String CONTENT_REPO_KEY = "Content Repository";
    public static final String COMPONENT_STATUS_REPO_KEY = "Component Status Repository";
    public static final String SECURITY_PROPS_KEY = "Security Properties";
    public static final String SENSITIVE_PROPS_KEY = "Sensitive Props";
    public static final String PROCESSORS_KEY = "Processors";
    public static final String CONNECTIONS_KEY = "Connections";
    public static final String PROVENANCE_REPORTING_KEY = "Provenance Reporting";
    public static final String REMOTE_PROCESSING_GROUPS_KEY = "Remote Processing Groups";
    public static final String INPUT_PORTS_KEY = "Input Ports";
    public static final String OUTPUT_PORTS_KEY = "Output Ports";
    public static final String PROVENANCE_REPO_KEY = "Provenance Repository";


    public static final String NAME_KEY = "name";
    public static final String COMMENT_KEY = "comment";
    public static final String ALWAYS_SYNC_KEY = "always sync";
    public static final String YIELD_PERIOD_KEY = "yield period";
    public static final String MAX_CONCURRENT_THREADS_KEY = "max concurrent threads";
    public static final String MAX_CONCURRENT_TASKS_KEY = "max concurrent tasks";
    public static final String ID_KEY = "id";
    public static final String SCHEDULING_STRATEGY_KEY = "scheduling strategy";
    public static final String SCHEDULING_PERIOD_KEY = "scheduling period";
    public static final String USE_COMPRESSION_KEY = "use compression";

    private CommonPropertyKeys() {
    }
}
