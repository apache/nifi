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

import java.io.IOException;
import java.util.List;

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.AbstractTarget;
import org.snmp4j.PDU;
import org.snmp4j.ScopedPDU;
import org.snmp4j.Snmp;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;

/**
 * Extension of {@link SNMPWorker} to perform SNMP Get and SNMP Walk requests.
 */
public final class SNMPGetter extends SNMPWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(SNMPGetter.class);
    private final OID oid;

    /**
     * Creates an instance of this getter.
     *
     * @param snmp   instance of {@link Snmp}
     * @param target instance of {@link AbstractTarget} to request
     * @param oid    instance of {@link OID} to request
     */
    public SNMPGetter(Snmp snmp, AbstractTarget target, OID oid) {
        super(snmp, target);
        this.oid = oid;
        LOGGER.info("Successfully initialized SNMP Getter");
    }

    /**
     * Construct the PDU to perform the SNMP Get request and returns
     * the result in order to create the flow file.
     *
     * @return {@link ResponseEvent}
     */
    public ResponseEvent get() {
        try {
            PDU pdu;
            if (target.getVersion() == SnmpConstants.version3) {
                pdu = new ScopedPDU();
            } else {
                pdu = new PDU();
            }
            pdu.add(new VariableBinding(oid));
            pdu.setType(PDU.GET);
            return snmp.get(pdu, target);
        } catch (IOException e) {
            LOGGER.error("Failed to get information from SNMP agent; {}", this, e);
            throw new ProcessException(e);
        }
    }

    /**
     * Perform a SNMP walk and returns the list of {@link TreeEvent}
     *
     * @return the list of {@link TreeEvent}
     */
    @SuppressWarnings("unchecked")
    public List<TreeEvent> walk() {
        TreeUtils treeUtils = new TreeUtils(snmp, new DefaultPDUFactory());
        return treeUtils.getSubtree(target, oid);
    }

    /**
     * @see SNMPWorker#toString()
     */
    @Override
    public String toString() {
        return super.toString() + ", OID:" + this.oid.toString();
    }
}
