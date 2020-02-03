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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.bouncycastle.openpgp.PGPPrivateKey;

/**
 * This class encapsulates a private key signing session.
 */
public class SignStreamSession {
    FlowFile flowFile;
    final ComponentLog logger;
    final PGPPrivateKey privateKey;
    final int signHashAlgorithm;
    final String attribute;
    final ProcessSession session;
    byte[] signature;

    SignStreamSession(ComponentLog logger, PGPPrivateKey privateKey, int signHashAlgorithm, String attribute, ProcessSession session, FlowFile flowFile) {
        this.logger = logger;
        this.privateKey = privateKey;
        this.signHashAlgorithm = signHashAlgorithm;
        this.attribute = attribute;
        this.session = session;
        this.flowFile = flowFile;
    }

    SignStreamSession(PGPPrivateKey privateKey, int signHashAlgorithm) {
        this(null, privateKey, signHashAlgorithm, "", null, null);
    }
}
