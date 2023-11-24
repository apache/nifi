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
package org.apache.nifi.security.cert;

/**
 * X.509 Certificate Subject Alternative Name
 */
public interface SubjectAlternativeName {
    /**
     * Get Name as a String which can return either the standard representation or an encoded representation of an ASN.1 byte array
     *
     * @return Subject Alternative Name string or encoded representation of bytes
     */
    String getName();

    /**
     * Get General Name Type descriptor for the Subject Alternative name
     *
     * @return General Name Type
     */
    GeneralNameType getGeneralNameType();
}
