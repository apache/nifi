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
package org.apache.nifi.pki;

import org.apache.nifi.controller.ControllerService;

import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.util.Optional;

/**
 * Private Key Service for finding Private Keys based on Certificate Parameters
 */
public interface PrivateKeyService extends ControllerService {
    /**
     * Find Private Key matching certificate serial number and issuer specified
     *
     * @param serialNumber Certificate Serial Number
     * @param issuer       X.500 Principal of Certificate Issuer
     * @return Private Key
     */
    Optional<PrivateKey> findPrivateKey(BigInteger serialNumber, X500Principal issuer);
}
