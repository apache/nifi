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

package org.apache.nifi.minifi.toolkit.schema.v1;

import org.apache.nifi.minifi.toolkit.schema.RemotePortSchema;
import org.apache.nifi.minifi.toolkit.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.toolkit.schema.common.StringUtil;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.REMOTE_PROCESS_GROUPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class RemoteProcessGroupSchemaV1 extends BaseSchema implements ConvertableSchema<RemoteProcessGroupSchema> {
    public static final String URL_KEY = "url";
    public static final String TIMEOUT_KEY = "timeout";

    public static final String DEFAULT_COMMENT = "";
    public static final String DEFAULT_TIMEOUT = "30 secs";
    public static final String DEFAULT_YIELD_PERIOD = "10 sec";

    private String name;
    private String url;
    private List<RemotePortSchema> inputPorts;

    private String comment = DEFAULT_COMMENT;
    private String timeout = DEFAULT_TIMEOUT;
    private String yieldPeriod = DEFAULT_YIELD_PERIOD;

    public RemoteProcessGroupSchemaV1(Map map) {
        name = getRequiredKeyAsType(map, NAME_KEY, String.class, REMOTE_PROCESS_GROUPS_KEY);
        String wrapperName = new StringBuilder("RemoteProcessingGroup(name: ").append(StringUtil.isNullOrEmpty(name) ? "unknown" : name).append(")").toString();
        url = getRequiredKeyAsType(map, URL_KEY, String.class, wrapperName);
        inputPorts = convertListToType(getRequiredKeyAsType(map, INPUT_PORTS_KEY, List.class, wrapperName), "input port", RemotePortSchema.class, INPUT_PORTS_KEY);
        if (inputPorts != null) {
            for (RemotePortSchema remoteInputPortSchema : inputPorts) {
                addIssuesIfNotNull(remoteInputPortSchema);
            }
        }

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, DEFAULT_COMMENT);
        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, wrapperName, DEFAULT_TIMEOUT);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, wrapperName, DEFAULT_YIELD_PERIOD);
    }

    @Override
    public RemoteProcessGroupSchema convert() {
        Map<String, Object> result = mapSupplier.get();
        result.put(NAME_KEY, name);
        result.put(URL_KEY, url);
        result.put(COMMENT_KEY, comment);
        result.put(TIMEOUT_KEY, timeout);
        result.put(YIELD_PERIOD_KEY, yieldPeriod);
        putListIfNotNull(result, INPUT_PORTS_KEY, inputPorts);
        return new RemoteProcessGroupSchema(result);
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public String getUrl() {
        return url;
    }

    public String getTimeout() {
        return timeout;
    }

    public String getYieldPeriod() {
        return yieldPeriod;
    }

    public List<RemotePortSchema> getInputPorts() {
        return inputPorts;
    }

    @Override
    public int getVersion() {
        return ConfigSchemaV1.CONFIG_VERSION;
    }
}
