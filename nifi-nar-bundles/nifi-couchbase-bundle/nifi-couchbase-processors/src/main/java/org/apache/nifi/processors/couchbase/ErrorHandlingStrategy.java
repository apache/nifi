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

import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Penalty.None;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Penalty.Penalize;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Penalty.Yield;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.Failure;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.ProcessException;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.Retry;


public enum ErrorHandlingStrategy {

    /**
     * Processor setting has to be fixed, in order to NOT call failing processor
     * frequently, this it be yielded.
     */
    ConfigurationError(ProcessException, Yield),
    /**
     * The input FlowFile will be sent to the failure relationship for further
     * processing without penalizing. Basically, the FlowFile shouldn't be sent
     * this processor again unless the issue has been solved.
     */
    InvalidInput(Failure, None),
    /**
     * Couchbase cluster is in unhealthy state. Retrying maybe successful,
     * but it should be yielded for a while.
     */
    TemporalClusterError(Retry, Yield),
    /**
     * The FlowFile was not processed successfully due to some temporal error
     * related to this specific FlowFile or document. Retrying maybe successful,
     * but it should be penalized for a while.
     */
    TemporalFlowFileError(Retry, Penalize),
    /**
     * The error can't be recovered without DataFlow Manager intervention.
     */
    Fatal(Retry, Yield);

    private final Result result;
    private final Penalty penalty;
    ErrorHandlingStrategy(Result result, Penalty penalty){
        this.result = result;
        this.penalty = penalty;
    }

    public enum Result {
        ProcessException, Failure, Retry
    }

    /**
     * Indicating yield or penalize the processing when transfer the input FlowFile.
     */
    public enum Penalty {
        Yield, Penalize, None
    }

    public Result result(){
        return this.result;
    }

    public Penalty penalty(){
        return this.penalty;
    }
}
