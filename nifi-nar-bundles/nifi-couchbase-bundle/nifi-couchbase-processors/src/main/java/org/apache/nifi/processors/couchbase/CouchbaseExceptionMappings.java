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
package org.apache.nifi.processors.couchbase;

import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.ConfigurationError;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Fatal;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.InvalidInput;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.TemporalClusterError;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.TemporalFlowFileError;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.BucketClosedException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.DocumentConcurrentlyModifiedException;
import com.couchbase.client.core.DocumentMutationLostException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.endpoint.SSLException;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.env.EnvironmentException;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.java.error.BucketDoesNotExistException;
import com.couchbase.client.java.error.CannotRetryException;
import com.couchbase.client.java.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.java.error.DurabilityException;
import com.couchbase.client.java.error.InvalidPasswordException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TranscodingException;

public class CouchbaseExceptionMappings {

    private static final Map<Class<? extends CouchbaseException>, ErrorHandlingStrategy>mapping = new HashMap<>();

    /*
     * - Won't happen
     * BucketAlreadyExistsException: never create a bucket
     * CASMismatchException: cas-id and replace is not used yet
     * DesignDocumentException: View is not used yet
     * DocumentAlreadyExistsException: insert is not used yet
     * DocumentDoesNotExistException: replace is not used yet
     * FlushDisabledException: never call flush
     * RepositoryMappingException: EntityDocument is not used
     * TemporaryLockFailureException: we don't obtain locks
     * ViewDoesNotExistException: View is not used yet
     * NamedPreparedStatementException: N1QL is not used yet
     * QueryExecutionException: N1QL is not used yet
     */
    static {
        /*
         * ConfigurationError
         */
        mapping.put(AuthenticationException.class, ConfigurationError);
        mapping.put(BucketDoesNotExistException.class, ConfigurationError);
        mapping.put(ConfigurationException.class, ConfigurationError);
        mapping.put(InvalidPasswordException.class, ConfigurationError);
        mapping.put(EnvironmentException.class, ConfigurationError);
        // when Couchbase doesn't have enough replica
        mapping.put(ReplicaNotConfiguredException.class, ConfigurationError);
        // when a particular Service(KV, View, Query, DCP) isn't running in a cluster
        mapping.put(ServiceNotAvailableException.class, ConfigurationError);
        // SSL configuration error, such as key store misconfiguration.
        mapping.put(SSLException.class, ConfigurationError);

        /*
         * InvalidInput
         */
        mapping.put(RequestTooBigException.class, InvalidInput);
        mapping.put(TranscodingException.class, InvalidInput);

        /*
         * Temporal Cluster Error
         */
        mapping.put(BackpressureException.class, TemporalClusterError);
        mapping.put(CouchbaseOutOfMemoryException.class, TemporalClusterError);
        mapping.put(TemporaryFailureException.class, TemporalClusterError);
        // occurs when a connection gets lost
        mapping.put(RequestCancelledException.class, TemporalClusterError);

        /*
         * Temporal FlowFile Error
         */
        mapping.put(DocumentConcurrentlyModifiedException.class, TemporalFlowFileError);
        mapping.put(DocumentMutationLostException.class, TemporalFlowFileError);
        mapping.put(DurabilityException.class, TemporalFlowFileError);

        /*
         * Fatal
         */
        mapping.put(BucketClosedException.class, Fatal);
        mapping.put(CannotRetryException.class, Fatal);
        mapping.put(NotConnectedException.class, Fatal);
    }

    /**
     * Returns a registered error handling strategy.
     * @param e the CouchbaseException
     * @return a registered strategy, if it's not registered, then return Fatal
     */
    public static ErrorHandlingStrategy getStrategy(CouchbaseException e){
        ErrorHandlingStrategy strategy = mapping.get(e.getClass());
        if(strategy == null) {
            // Treat unknown Exception as Fatal.
            return ErrorHandlingStrategy.Fatal;
        }
        return strategy;
    }

}
