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
package org.apache.nifi.properties.sensitive;

/**
 * Parent interface to allow provider-specific encryption metadata around protected property values.
 */
public interface SensitivePropertyMetadata {

    /**
     * Returns the String to use to identify the protected vaue in the {@code .protected} property. Depending on the implementation, this may include an algorithm & mode, a key ID, KMS URL, etc.
     * @return the identifier
     */
    String getIdentifier();
}
