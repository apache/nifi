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


import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * This interface describes callback methods used by the ExecuteScript/InvokeScript processors to perform
 * engine-specific tasks at various points in the engine lifecycle.
 */
public interface ScriptEngineConfigurator {

    String getScriptEngineName();

    Object init(ScriptEngine engine, String modulePath) throws ScriptException;

    Object eval(ScriptEngine engine, String scriptBody, String modulePath) throws ScriptException;

}
