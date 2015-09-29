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

import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Penalty.Penalize;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Penalty.Yield;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.Failure;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.ProcessException;
import static org.apache.nifi.processors.couchbase.ErrorHandlingStrategy.Result.Retry;


public enum ErrorHandlingStrategy {

    ConfigurationError(ProcessException, Yield),
    InvalidInput(Failure, Penalize),
    TemporalClusterError(Retry, Yield),
    TemporalFlowFileError(Retry, Penalize),
    Fatal(Failure, Yield);

    private final Result result;
    private final Penalty penalty;
    private ErrorHandlingStrategy(Result result, Penalty penalty){
        this.result = result;
        this.penalty = penalty;
    }

    public enum Result {
        ProcessException, Failure, Retry;
    }

    /**
     * Indicating yield or penalize the processing when transfer the input FlowFile.
     */
    public enum Penalty {
        Yield, Penalize;
    }

    public Result result(){
        return this.result;
    }

    public Penalty penalty(){
        return this.penalty;
    }
}
