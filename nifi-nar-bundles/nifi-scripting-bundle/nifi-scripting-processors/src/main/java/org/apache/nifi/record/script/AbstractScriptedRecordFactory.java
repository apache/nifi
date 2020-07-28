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
package org.apache.nifi.record.script;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.AbstractScriptedControllerService;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract base class containing code common to the Scripted record reader/writer implementations
 */
public abstract class AbstractScriptedRecordFactory<T> extends AbstractScriptedControllerService {

    protected final AtomicReference<T> recordFactory = new AtomicReference<>();

    public void setup() {
        // Create a single script engine, the Processor object is reused by each task
        if (scriptEngine == null) {
            scriptingComponentHelper.setup(1, getLogger());
            scriptEngine = scriptingComponentHelper.engineQ.poll();
        }

        if (scriptEngine == null) {
            throw new ProcessException("No script engine available!");
        }

        if (scriptNeedsReload.get() || recordFactory.get() == null) {
            if (ScriptingComponentHelper.isFile(scriptingComponentHelper.getScriptPath())) {
                reloadScriptFile(scriptingComponentHelper.getScriptPath());
            } else {
                reloadScriptBody(scriptingComponentHelper.getScriptBody());
            }
            scriptNeedsReload.set(false);
        }
    }
}
