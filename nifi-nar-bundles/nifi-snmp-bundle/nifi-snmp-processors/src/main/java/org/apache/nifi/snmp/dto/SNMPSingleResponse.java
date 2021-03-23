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
package org.apache.nifi.snmp.dto;

import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.Target;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SNMPSingleResponse {

    private final Target target;
    private final PDU responsePdu;

    public SNMPSingleResponse(final Target target, final PDU responsePdu) {
        this.target = target;
        this.responsePdu = responsePdu;
    }

    public boolean isValid() {
        return responsePdu.getErrorStatus() == PDU.noError;
    }


    public Map<String, String> getAttributes() {
        return SNMPUtils.getPduAttributeMap(responsePdu);
    }

    public List<SNMPValue> getVariableBindings() {
        return responsePdu.getVariableBindings()
                .stream()
                .map(p -> new SNMPValue(p.getOid().toString(), p.getVariable().toString()))
                .collect(Collectors.toList());
    }

    public String getErrorStatusText() {
        return responsePdu.getErrorStatusText();
    }

    public String getTargetAddress() {
        return target.getAddress().toString();
    }

    public int getVersion() {
        return target.getVersion();
    }

    public boolean isReportPdu() {
        return responsePdu.getType() == PDU.REPORT;
    }
}
