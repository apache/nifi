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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.smi.Integer32;

import java.io.IOException;

public class SNMPResourceHandler {

    public static final String INVALID_FLOWFILE_EXCEPTION_MESSAGE = "Could not read the variable bindings from the " +
            "flowfile. Please, add the OIDs to set in separate properties. E.g. Property name: snmp$1.3.6.1.2.1.1.1.0 " +
            "Value: Example value. ";

    public static final String REQUEST_TIMEOUT_EXCEPTION_TEMPLATE = "Request timed out. Please check if (1). the " +
            "agent host and port is correctly set, (2). the agent is running, (3). the agent SNMP version corresponds" +
            " with the processor's one, (4) the community string is correct and has %1$s access, (5) In case of SNMPv3" +
            " check if the user credentials are valid and the user in a group with %1$s access.";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Snmp snmpManager;
    private final Target target;

    public SNMPResourceHandler(final Snmp snmpManager, final Target target) {
        this.snmpManager = snmpManager;
        this.target = target;
    }

    public Snmp getSnmpManager() {
        return snmpManager;
    }

    public Target getTarget() {
        return target;
    }

    public void close() {
        try {
            if (snmpManager.getUSM() != null) {
                snmpManager.getUSM().removeAllUsers();
                SecurityModels.getInstance().removeSecurityModel(new Integer32(snmpManager.getUSM().getID()));
            }
            snmpManager.close();
        } catch (IOException e) {
            final String errorMessage = "Could not close SNMP manager.";
            logger.error(errorMessage, e);
            throw new ProcessException(errorMessage);
        }
    }
}
