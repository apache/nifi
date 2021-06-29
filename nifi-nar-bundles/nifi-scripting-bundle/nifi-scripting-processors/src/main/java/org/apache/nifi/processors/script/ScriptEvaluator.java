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
package org.apache.nifi.processors.script;

import org.apache.nifi.serialization.record.Record;

import javax.script.ScriptException;

/**
 * Used by scripted record processors to enclose script engines for different languages.
 */
interface ScriptEvaluator {

    /**
     * Evaluates the enclosed script using the record as argument. Returns with the script's return value.
     *
     * @param record The script to evaluate.
     * @param index The index of the record.
     *
     * @return The return value of the evaluated script.
     *
     * @throws ScriptException In case of issues with the evaluations.
     */
    Object evaluate(Record record, long index) throws ScriptException;
}
