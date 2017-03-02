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
package org.apache.nifi.snmp.processors;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.AbstractTarget;
import org.snmp4j.Snmp;

/**
 * Base class for implementing SNMP workers.
 *
 * @see SNMPSetter
 * @see SNMPGetter
 */
abstract class SNMPWorker implements AutoCloseable {

    /** logger */
    private final static Logger logger = LoggerFactory.getLogger(SNMPWorker.class);

    /** SNMP abstraction */
    protected final Snmp snmp;

    /** Target to request */
    protected final AbstractTarget target;

    /**
     * Creates an instance of this worker and initializing it with {@link Snmp}
     * and {@link AbstractTarget} used by sub-classes to interact with SNMP agent.
     * @param snmp instance of {@link Snmp}
     * @param target instance of {@link AbstractTarget}
     */
    public SNMPWorker(Snmp snmp, AbstractTarget target) {
        this.snmp = snmp;
        this.target = target;
    }

    /**
     * Closes {@link Snmp} created when instance of this class was created.
     */
    @Override
    public void close() throws TimeoutException, IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Closing SNMP connection");
        }
        this.snmp.close();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + this.snmp.toString();
    }

}
