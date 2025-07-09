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

import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketNotFoundDuringLoadException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentMutationLostException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.InvalidRequestException;
import com.couchbase.client.core.error.ReplicaNotConfiguredException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.json.MapperException;

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
        mapping.put(AuthenticationFailureException.class, ConfigurationError);
        mapping.put(BucketNotFoundDuringLoadException.class, ConfigurationError);
        mapping.put(ScopeNotFoundException.class, ConfigurationError);
        mapping.put(CollectionNotFoundException.class, ConfigurationError);
        mapping.put(ConfigException.class, ConfigurationError);
        // when Couchbase doesn't have enough replica
        mapping.put(ReplicaNotConfiguredException.class, ConfigurationError);
        // when a particular Service(KV, View, Query, DCP) isn't running in a cluster
        mapping.put(ServiceNotAvailableException.class, ConfigurationError);

        /*
         * InvalidInput
         */
        mapping.put(InvalidRequestException.class, InvalidInput);
        mapping.put(ValueTooLargeException.class, InvalidInput);
        mapping.put(MapperException.class, InvalidInput);

        /*
         * Temporal Cluster Error
         */
        mapping.put(ServerOutOfMemoryException.class, TemporalClusterError);
        mapping.put(TemporaryFailureException.class, TemporalClusterError);
        // occurs when a connection gets lost
        mapping.put(RequestCanceledException.class, TemporalClusterError);

        /*
         * Temporal FlowFile Error
         */
        mapping.put(CasMismatchException.class, TemporalFlowFileError);
        mapping.put(DocumentMutationLostException.class, TemporalFlowFileError);
        mapping.put(DurabilityImpossibleException.class, TemporalFlowFileError);
        mapping.put(DurabilityAmbiguousException.class, TemporalFlowFileError);
        mapping.put(DurabilityLevelNotAvailableException.class, TemporalFlowFileError);

        /*
         * Fatal
         */
        mapping.put(AlreadyShutdownException.class, Fatal);
        mapping.put(UnambiguousTimeoutException.class, Fatal);
    }

    /**
     * Returns a registered error handling strategy.
     * @param e the CouchbaseException
     * @return a registered strategy, if it's not registered, then return Fatal
     */
    public static ErrorHandlingStrategy getStrategy(CouchbaseException e) {
        ErrorHandlingStrategy strategy = mapping.get(e.getClass());
        if (strategy == null) {
            // Treat unknown Exception as Fatal.
            return ErrorHandlingStrategy.Fatal;
        }
        return strategy;
    }

}
