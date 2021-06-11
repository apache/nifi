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


import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

public interface ScriptRunner {

    String getScriptEngineName();

    ScriptEngine getScriptEngine();

    /**
     * Runs the script held by this ScriptRunner using the provided Bindings map (to include the session object for example)
     *
     * @param bindings The Bindings the underlying engine should use when running the script
     * @throws ScriptException if an error occurs during execution of the script
     */
    void run(Bindings bindings) throws ScriptException;

}
