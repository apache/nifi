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

import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Abstraction for reading attributes of an X.509 certificate
 */
public interface CertificateAttributeReader {
    /**
     * Get Subject Alternative Names or empty when none found
     *
     * @param certificate X.509 certificate to be read
     * @return List of Subject Alternative Names
     */
    List<SubjectAlternativeName> getSubjectAlternativeNames(X509Certificate certificate);
}
