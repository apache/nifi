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
package org.apache.nifi.security.crypto.key;

/**
 * Derived Key Specification with password and parameters for implementing algorithms
 */
public interface DerivedKeySpec<T extends DerivedKeyParameterSpec> {
    /**
     * Get password characters that algorithms use for key derivation
     *
     * @return Password characters
     */
    char[] getPassword();

    /**
     * Get length requested for derived key in bytes
     *
     * @return Length of derived key in bytes
     */
    int getDerivedKeyLength();

    /**
     * Get cipher algorithm for which the derived key will be used
     *
     * @return Cipher algorithm
     */
    String getAlgorithm();

    /**
     * Get parameter specification
     *
     * @return Parameter specification
     */
    T getParameterSpec();
}
