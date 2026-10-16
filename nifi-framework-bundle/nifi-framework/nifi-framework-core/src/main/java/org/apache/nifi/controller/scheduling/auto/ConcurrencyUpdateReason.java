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
package org.apache.nifi.controller.scheduling.auto;

public enum ConcurrencyUpdateReason {
    COLLECTING_PERFORMANCE_MEASUREMENTS,
    REQUIRES_SINGLE_CONCURRENT_TASK,
    CONCURRENT_TASK_LIMIT_REACHED,
    CPU_LIMIT_REACHED,
    CONFIGURED_MAX_CONCURRENT_TASKS_REACHED,
    TESTING_HIGHER_CONCURRENCY,
    TESTING_LOWER_CONCURRENCY,
    PROCESSOR_INVOCATION_FAILED,
    GARBAGE_COLLECTION_TIME_TOO_HIGH,
    CPU_LOAD_TOO_HIGH,
    GLOBAL_CAPACITY_FULL,
    NO_WAITING_WORK,
    PROCESSOR_NOT_READY,
    DOWNSTREAM_BACK_PRESSURE,
    PROCESSOR_YIELDING,
    CONCURRENT_TASKS_UNDERUSED,
    PREVIOUS_INCREASE_DID_NOT_IMPROVE_THROUGHPUT,
    COOLDOWN
}
