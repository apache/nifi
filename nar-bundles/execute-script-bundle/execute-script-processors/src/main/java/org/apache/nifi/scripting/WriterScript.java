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
package org.apache.nifi.scripting;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptException;

/**
 * <p>
 * Script authors should extend this class if they want to follow the
 * "processCallback" paradigm for NiFi processors.
 * </p>
 *
 * <p>
 * At a minimum, scripts must implement
 * <code>process(FileInputStream, FileOutputStream)</code>.
 * </p>
 *
 * <p>
 * By default, all files processed will be sent to the relationship
 * <em>success</em>, unless the scriptFileName raises an exception, in which
 * case the file will be sent to <em>failure</em>. Implement
 * {@link #getProcessorRelationships()} and/or {@link #getRoute()} to change
 * this behavior.
 * </p>
 *
 */
public class WriterScript extends Script {

    private Object processCallback;

    public WriterScript() {

    }

    public WriterScript(Object... callbacks) {
        super(callbacks);
        for (Object callback : callbacks) {
            if (callback instanceof Map<?, ?>) {
                processCallback = processCallback == null && ((Map<?, ?>) callback).containsKey("process") ? callback : processCallback;
            }
        }
    }

    public void process(InputStream in, OutputStream out) throws NoSuchMethodException, ScriptException {
        Invocable inv = (Invocable) engine;
        inv.invokeMethod(processCallback, "process", in, out);
    }
}
