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
package org.apache.nifi.security.pgp;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * The PGPKeyMaterialService is responsible for providing PGP Content Processors the mechanisms for applying the
 * various PGP operations to {@link FlowFile}s.
 *
 */
public interface PGPKeyMaterialService extends ControllerService {
    /**
     * Encrypts a {@link FlowFile}.
     *
     * @param flow flow to encrypt
     * @param context properties for encryption options
     * @param session processing session
     * @return encrypted flow
     */
    FlowFile encrypt(FlowFile flow, PropertyContext context, ProcessSession session);

    /**
     * Decrypts a {@link FlowFile}.
     *
     * @param flow flow to decrypt
     * @param context properties for the decryption options
     * @param session processing session
     * @return decrypted flow
     */
    FlowFile decrypt(FlowFile flow, PropertyContext context, ProcessSession session);

    /**
     * Signs a {@link FlowFile}.
     *
     * @param flow flow to sign
     * @param context properties for signing options
     * @param session processing session
     * @return signature output stream
     */
    byte[] sign(FlowFile flow, PropertyContext context, ProcessSession session) throws ProcessException;

    /**
     * Verify a {@link FlowFile}.
     *
     * @param flow flow to verify
     * @param context properties for verification session
     * @param session processing session
     * @return true if the flow can be verified with signature, false if cannot be verified
     */
    boolean verify(FlowFile flow, PropertyContext context, ProcessSession session) throws ProcessException;
}
