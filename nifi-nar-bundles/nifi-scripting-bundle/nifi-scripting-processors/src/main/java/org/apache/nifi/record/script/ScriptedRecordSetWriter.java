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

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * A RecordSetWriter implementation that allows the user to script the RecordWriter instance
 */
@Tags({"record", "writer", "script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj"})
@CapabilityDescription("Allows the user to provide a scripted RecordSetWriterFactory instance in order to write records to an outgoing flow file.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
public class ScriptedRecordSetWriter extends AbstractScriptedRecordFactory<RecordSetWriterFactory> implements RecordSetWriterFactory {

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        super.onEnabled(context);
    }


    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) throws SchemaNotFoundException, IOException {
        if (recordFactory.get() != null) {
            try {
                return recordFactory.get().createWriter(logger, schema, out, variables);
            } catch (UndeclaredThrowableException ute) {
                throw new IOException(ute.getCause());
            }
        }
        return null;
    }


    /**
     * Reloads the script RecordSetWriterFactory. This must be called within the lock.
     *
     * @param scriptBody An input stream associated with the script content
     * @return Whether the script was successfully reloaded
     */
    @Override
    protected boolean reloadScript(final String scriptBody) {
        // note we are starting here with a fresh listing of validation
        // results since we are (re)loading a new/updated script. any
        // existing validation results are not relevant
        final Collection<ValidationResult> results = new HashSet<>();

        try {
            // Create a single script engine, the Processor object is reused by each task
            if (scriptRunner == null) {
                scriptingComponentHelper.setupScriptRunners(1, scriptBody, getLogger());
                scriptRunner = scriptingComponentHelper.scriptRunnerQ.poll();
            }

            if (scriptRunner == null) {
                throw new ProcessException("No script runner available!");
            }
            // get the engine and ensure its invocable
            ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
            if (scriptEngine instanceof Invocable) {
                final Invocable invocable = (Invocable) scriptEngine;

                // evaluate the script
                scriptRunner.run(scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE));


                // get configured processor from the script (if it exists)
                final Object obj = scriptRunner.getScriptEngine().get("writer");
                if (obj != null) {
                    final ComponentLog logger = getLogger();

                    try {
                        // set the logger if the processor wants it
                        invocable.invokeMethod(obj, "setLogger", logger);
                    } catch (final NoSuchMethodException nsme) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Configured script RecordSetWriterFactory does not contain a setLogger method.");
                        }
                    }

                    if (configurationContext != null) {
                        try {
                            // set the logger if the processor wants it
                            invocable.invokeMethod(obj, "setConfigurationContext", configurationContext);
                        } catch (final NoSuchMethodException nsme) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Configured script RecordSetWriterFactory does not contain a setConfigurationContext method.");
                            }
                        }
                    }

                    // record the processor for use later
                    final RecordSetWriterFactory scriptedWriter = invocable.getInterface(obj, RecordSetWriterFactory.class);
                    recordFactory.set(scriptedWriter);

                } else {
                    throw new ScriptException("No RecordSetWriterFactory was defined by the script.");
                }
            }

        } catch (final Exception ex) {
            final ComponentLog logger = getLogger();
            final String message = "Unable to load script: " + ex.getLocalizedMessage();

            logger.error(message, ex);
            results.add(new ValidationResult.Builder()
                    .subject("ScriptValidation")
                    .valid(false)
                    .explanation("Unable to load script due to " + ex.getLocalizedMessage())
                    .input(scriptingComponentHelper.getScriptPath())
                    .build());
        }

        // store the updated validation results
        validationResults.set(results);

        // return whether there was any issues loading the configured script
        return results.isEmpty();
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final RecordSetWriterFactory writerFactory = recordFactory.get();
        if (writerFactory == null) {
            return null;
        }

        try {
            return writerFactory.getSchema(variables, readSchema);
        } catch (UndeclaredThrowableException ute) {
            throw new IOException(ute.getCause());
        }
    }
}
