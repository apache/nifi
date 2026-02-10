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

package org.apache.nifi.components.connector;

/**
 * Runtime exception thrown by {@link ConnectorConfigurationProvider} implementations
 * to indicate a failure in an external configuration operation such as load, save, discard, or delete.
 *
 * <p>This exception type allows provider implementations to signal failures using a well-defined
 * exception rather than generic exceptions like IOException or database-specific exceptions.
 * The framework will propagate these exceptions to callers rather than handling them silently.</p>
 */
public class ConnectorConfigurationProviderException extends RuntimeException {

    public ConnectorConfigurationProviderException(final String message) {
        super(message);
    }

    public ConnectorConfigurationProviderException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
