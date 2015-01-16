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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.script.ScriptException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.BufferedInputStream;
import org.apache.nifi.io.BufferedOutputStream;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scripting.ConverterScript;
import org.apache.nifi.scripting.ReaderScript;
import org.apache.nifi.scripting.Script;
import org.apache.nifi.scripting.ScriptFactory;
import org.apache.nifi.scripting.WriterScript;

/**
 * <!-- Processor Documentation ================================================== -->
 * <h2>Description:</h2>
 * <p>
 * This processor provides the capability to execute scripts in various
 * scripting languages, and passes into the scripts the input stream and output
 * stream(s) representing an incoming flow file and any created flow files. The
 * processor is designed to be thread safe, so multiple concurrent tasks may
 * execute against a single script. The processor provides a framework which
 * enables script writers to implement 3 different types of scripts:
 * <ul>
 * ReaderScript - which enables stream-based reading of a FlowFile's
 * content</br> WriterScript - which enables stream-based reading and
 * writing/modifying of a FlowFile's content</br> ConverterScript - which
 * enables stream-based reading a FlowFile's content and stream-based writing to
 * newly created FlowFiles</br>
 * </ul>
 * Presently, the processor supports 3 scripting languages: Ruby, Python, and
 * JavaScript. The processor is built on the javax.script API which enables
 * ScriptEngine discovery, thread management, and encapsulates much of the low
 * level bridging-code that enables Java to Script language integration. Thus,
 * it is designed to be easily extended to other scripting languages. </br> The
 * attributes of a FlowFile and properties of the Processor are exposed to the
 * script by either a variable in the base class or a getter method. A script
 * may declare new Processor Properties and different Relationships via
 * overriding the getPropertyDescriptors and getRelationships methods,
 * respectively.
 * </p>
 * <p>
 * <strong>Properties:</strong>
 * </p>
 * <p>
 * In the list below, the names of required properties appear in bold. Any other
 * properties (not in bold) are considered optional. If a property has a default
 * value, it is indicated. If a property supports the use of the NiFi Expression
 * Language (or simply, "expression language"), that is also indicated. Of
 * particular note: This processor allows scripts to define additional Processor
 * properties, which will not be initially visible. Once the processor's
 * configuration is validated, script defined properties will become visible,
 * and may affect the validity of the processor.
 * </p>
 * <ul>
 * <li>
 * <strong>Script File Name</strong>
 * <ul>
 * <li>Script location, can be relative or absolute path.</li>
 * <li>Default value: no default</li>
 * <li>Supports expression language: false</li>
 * </ul>
 * </li>
 * <li>
 * <strong>Script Check Interval</strong>
 * <ul>
 * <li>The time period between checking for updates to a script.</li>
 * <li>Default value: 15 sec</li>
 * <li>Supports expression language: false</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <strong>Relationships:</strong>
 * </p>
 * <p>
 * The initial 'out of the box' relationships are below. Of particular note is
 * the ability of a script to change the set of relationships. However, any
 * relationships defined by the script will not be visible until the processor's
 * configuration has been validated. Once done, new relationships will become
 * visible.
 * </p>
 * <ul>
 * <li>
 * success
 * <ul>
 * <li>Used when a file is successfully processed by a script.</li>
 * </ul>
 * </li>
 * <li>
 * failure
 * <ul>
 * <li>Used when an error occurs while processing a file with a script.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <strong>Example Scripts:</strong>
 * </p>
 * <ul>
 * JavaScript example - the 'with' statement imports packages defined in the
 * framework. Since the 'instance' variable is intended to be local scope (not
 * global), it must be named 'instance' as it it not passed back to the
 * processor upon script evaluation and must be fetched. If you make it global,
 * you can name it whatever you'd like...but this is intended to be
 * multi-threaded so do so at your own risk. Presently, there are issues with
 * the JavaScript scripting engine that prevent sub-classing the base classes in
 * the Processor's Java framework. So, what is actually happening is an instance
 * of the ReaderScript is created with a provided callback object. When we are
 * able to move to a more competent scripting engine, the code below will remain
 * the same, but the 'instance' variable will actually be a sub-class of
 * ReaderScript.
 *
 * <pre>
 *               with (Scripting) {
 *                 var instance = new ReaderScript({
 *                     route : function(input) {
 *                         var str = IOUtils.toString(input);
 *                         var expr = instance.getProperty("expr");
 *                         filename = instance.attributes.get("filename");
 *                         instance.setAttribute("filename", filename + ".modified");
 *                         if (str.match(expr)) {
 *                             return Script.FAIL_RELATIONSHIP;
 *                         } else {
 *                             return Script.SUCCESS_RELATIONSHIP;
 *                         }
 *                     }
 *                 });
 *               }
 * </pre>
 *
 * Ruby example - the 'OutputStreamHandler' is an interface which is called when
 * creating flow files.
 *
 * <pre>
 *                 java_import 'org.apache.nifi.scripting.OutputStreamHandler'
 *                 class SimpleConverter < ConverterScript
 *                   field_reader :FAIL_RELATIONSHIP, :SUCCESS_RELATIONSHIP, :logger, :attributes
 *
 *                   def convert(input)
 *                     in_io = input.to_io
 *                     createFlowFile("firstLine", FAIL_RELATIONSHIP, OutputStreamHandler.impl do |method, out|
 *                         out_io = out.to_io
 *                         out_io << in_io.readline.to_java_bytes
 *                         out_io.close
 *                         logger.debug("Wrote data to failure...this message logged with logger from super class")
 *                       end)
 *
 *                     createFlowFile("otherLines", SUCCESS_RELATIONSHIP, OutputStreamHandler.impl do |method, out|
 *                         out_io = out.to_io
 *                         in_io.each_line { |line|
 *                           out_io << line
 *                         }
 *                         out_io.close
 *                         logger.debug("Wrote data to success...this message logged with logger from super class")
 *                       end)
 *                     in_io.close
 *                   end
 *
 *                 end
 *
 *                 $logger.debug("Creating SimpleConverter...this message logged with logger from shared variables")
 *                 SimpleConverter.new
 * </pre>
 *
 * Python example - The difficulty with Python is that it does not return
 * objects upon script evaluation, so the instance of the Script class must be
 * fetched by name. Thus, you must define a variable called 'instance'.
 *
 * <pre>
 *                 import re
 *
 *                 class RoutingReader(ReaderScript):
 *                     A = Relationship.Builder().name("a").description("some good stuff").build()
 *                     B = Relationship.Builder().name("b").description("some other stuff").build()
 *                     C = Relationship.Builder().name("c").description("some bad stuff").build()
 *
 *                     def getRelationships(self):
 *                         return [self.A,self.B,self.C]
 *
 *                     def getExceptionRoute(self):
 *                         return self.C
 *
 *                     def route( self, input ):
 *                         for line in FileUtil.wrap(input):
 *                             if re.match("^bad", line, re.IGNORECASE):
 *                                 return self.B
 *                             if re.match("^sed", line):
 *                                 raise RuntimeError("That's no good!")
 *
 *                         return self.A
 *
 *                 instance = RoutingReader()
 * </pre>
 *
 * </ul>
 * <p>
 * <strong>Shared Variables</strong>
 * </p>
 * <ul>
 * <li>logger : global scope</li>
 * <li>properties : local/instance scope</li>
 * </ul>
 * <p>
 * <strong>Script API:</strong>
 * </p>
 * <ul>
 * <li>getAttribute(String) : String</li>
 * <li>getAttributes() : Map(String,String)</li>
 * <li>getExceptionRoute() : Relationship</li>
 * <li>getFileName() : String</li>
 * <li>getFlowFileEntryDate() : Calendar</li>
 * <li>getFlowFileSize() : long</li>
 * <li>getProperties() : Map(String, String)</li>
 * <li>getProperty(String) : String</li>
 * <li>getPropertyDescriptors() : List(PropertyDescriptor)</li>
 * <li>getRelationships() : Collection(Relationship)</li>
 * <li>getRoute() : Relationship</li>
 * <li>setRoute(Relationship)</li>
 * <li>setAttribute(String, String)</li>
 * <li>validate() : Collection(String)</li>
 * </ul>
 * <p>
 * <strong>ReaderScript API:</strong>
 * </p>
 * <ul>
 * <li>route(InputStream) : Relationship</li>
 * </ul>
 * <p>
 * <strong>WriterScript API:</strong>
 * </p>
 * <ul>
 * <li>process(InputStream, OutputStream)</li>
 * </ul>
 * <p>
 * <strong>ConverterScript API:</strong>
 * </p>
 * <ul>
 * <li>convert(InputStream)</li>
 * <li>createFlowFile(String, Relationship, OutputStreamHandler)</li>
 * </ul>
 * <p>
 * <strong>OutputStreamHandler API:</strong>
 * </p>
 * <ul>
 * <li>write(OutputStream)</li>
 * </ul>
 */
@EventDriven
@Tags({"script", "ruby", "python", "javascript", "execute"})
@CapabilityDescription("Execute scripts in various scripting languages, and passes into the scripts the input stream and output stream(s) "
        + "representing an incoming flow file and any created flow files.")
public class ExecuteScript extends AbstractProcessor {

    private final AtomicBoolean doCustomValidate = new AtomicBoolean(true);
    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private final AtomicReference<List<PropertyDescriptor>> propertyDescriptors = new AtomicReference<>();
    private volatile ScriptFactory scriptFactory;
    private volatile Relationship exceptionRoute;

    /**
     * Script location, can be relative or absolute path -- passed as-is to
     * {@link File#File(String) File constructor}
     */
    public static final PropertyDescriptor SCRIPT_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Script File Name")
            .description("Script location, can be relative or absolute path")
            .required(true)
            .addValidator(new Validator() {

                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    ValidationResult result = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, input, context);
                    if (result.isValid()) {
                        int dotPos = input.lastIndexOf('.');
                        if (dotPos < 1) {
                            result = new ValidationResult.Builder()
                            .subject(subject)
                            .valid(false)
                            .explanation("Filename must have an extension")
                            .input(input)
                            .build();
                        }
                    }
                    return result;
                }
            })
            .build();

    static final PropertyDescriptor SCRIPT_CHECK_INTERVAL = new PropertyDescriptor.Builder()
            .name("Script Check Interval")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .description("The time period between checking for updates to a script")
            .required(true)
            .defaultValue("15 sec")
            .build();

    @Override
    protected void init(ProcessorInitializationContext context) {
        Set<Relationship> empty = Collections.emptySet();
        relationships.set(empty);
        ArrayList<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(SCRIPT_FILE_NAME);
        propDescs.add(SCRIPT_CHECK_INTERVAL);
        propertyDescriptors.set(Collections.unmodifiableList(propDescs));
        scriptFactory = new ScriptFactory(getLogger());
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors.get();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .dynamic(true)
                .addValidator(Validator.VALID)
                .build();
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        doCustomValidate.set(true);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    /**
     * Called by framework.
     *
     * Returns a list of reasons why this processor cannot be run.
     * @return 
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        if (doCustomValidate.getAndSet(false)) {
            long interval = validationContext.getProperty(SCRIPT_CHECK_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
            scriptFactory.setScriptCheckIntervalMS(interval);
            List<ValidationResult> results = new ArrayList<>();
            String file = validationContext.getProperty(SCRIPT_FILE_NAME).getValue();
            try {
                Script s = scriptFactory.getScript(file);

                // set the relationships of the processor
                relationships.set(new HashSet<>(s.getRelationships()));

                // need to get script's prop. descs. and validate. May, or may not, have dynamic
                // props already...depends if this is the first time the processor is being configured.
                Map<PropertyDescriptor, String> properties = validationContext.getProperties();

                // need to compare props, if any, against script-expected props that are required.
                // script may be expecting required props that are not known, or some props may have invalid
                // values.
                // processor may be configured with dynamic props that the script will use...but does not declare which would
                // be a bad thing
                List<PropertyDescriptor> scriptPropDescs = s.getPropertyDescriptors();
                getLogger().debug("Script is {}", new Object[]{s});
                getLogger().debug("Script file name is {}", new Object[]{s.getFileName()});
                getLogger().debug("Script Prop Descs are: {}", new Object[]{scriptPropDescs.toString()});
                getLogger().debug("Thread is: {}", new Object[]{Thread.currentThread().toString()});
                for (PropertyDescriptor propDesc : scriptPropDescs) {
                    // need to check for missing props
                    if (propDesc.isRequired() && !properties.containsKey(propDesc)) {
                        results.add(new ValidationResult.Builder()
                                .subject("Script Properties")
                                .valid(false)
                                .explanation("Missing Property " + propDesc.getName())
                                .build());

                        // need to validate current value against script provided validator
                    } else if (properties.containsKey(propDesc)) {
                        String value = properties.get(propDesc);
                        ValidationResult result = propDesc.validate(value, validationContext);
                        if (!result.isValid()) {
                            results.add(result);
                        }
                    } // else it is an optional prop according to the script and it is not specified by
                    // the configuration of the processor
                }

                // need to update the known prop desc's with what we just got from the script
                List<PropertyDescriptor> pds = new ArrayList<>(propertyDescriptors.get());
                pds.addAll(scriptPropDescs);
                propertyDescriptors.set(Collections.unmodifiableList(pds));

                if (results.isEmpty()) {
                    // so needed props are supplied and individually validated, now validate script
                    Collection<String> reasons;
                    reasons = s.validate();
                    if (null == reasons) {
                        getLogger().warn("Script had invalid return value for validate(), ignoring.");
                    } else {
                        for (String reason : reasons) {
                            ValidationResult result = new ValidationResult.Builder()
                                    .subject("ScriptValidation")
                                    .valid(false)
                                    .explanation(reason)
                                    .build();
                            results.add(result);
                        }
                    }
                }

                // get the exception route
                exceptionRoute = s.getExceptionRoute();

                return results;
            } catch (ScriptException | IOException | NoSuchMethodException e) {
                doCustomValidate.set(true);
                results.add(new ValidationResult.Builder()
                        .subject("ScriptValidation")
                        .valid(false)
                        .explanation("Cannot create script due to " + e.getMessage())
                        .input(file)
                        .build());
                getLogger().error("Cannot create script due to " + e, e);
                return results;
            }
        }
        return null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return; // fail-fast if there is no work to do
        }

        final String scriptFileName = context.getProperty(SCRIPT_FILE_NAME).getValue();
        // doing this cloning because getProperties does not initialize props that have only their default values
        // must do a getProperty for that value to be initialized
        Map<String, String> props = new HashMap<>();
        for (PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (propDesc.isExpressionLanguageSupported()) {
                props.put(propDesc.getName(), context.getProperty(propDesc).evaluateAttributeExpressions(flowFile).getValue());
            } else {
                props.put(propDesc.getName(), context.getProperty(propDesc).getValue());
            }
        }
        Script script = null;
        try {
            final Script finalScript = scriptFactory.getScript(scriptFileName, props, flowFile);
            script = finalScript;
            if (finalScript instanceof ReaderScript) {
                session.read(flowFile, new InputStreamCallback() {

                    @Override
                    public void process(InputStream in) throws IOException {
                        try {
                            ((ReaderScript) finalScript).process(new BufferedInputStream(in));
                        } catch (NoSuchMethodException | ScriptException e) {
                            getLogger().error("Failed to execute ReaderScript", e);
                            throw new IOException(e);
                        }
                    }
                });
            } else if (finalScript instanceof WriterScript) {
                flowFile = session.write(flowFile, new StreamCallback() {

                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        try {
                            ((WriterScript) finalScript).process(new BufferedInputStream(in), new BufferedOutputStream(out));
                            out.flush();
                        } catch (NoSuchMethodException | ScriptException e) {
                            getLogger().error("Failed to execute WriterScript", e);
                            throw new IOException(e);
                        }
                    }
                });
            } else if (finalScript instanceof ConverterScript) {
                ((ConverterScript) finalScript).process(session);

                // Note that these scripts don't pass the incoming FF through,
                // they always create new outputs
                session.remove(flowFile);
                return;
            } else {
                // only thing we can do is assume script has already run and done it's thing, so just transfer the incoming
                // flowfile
                getLogger().debug("Successfully executed script from {}", new Object[]{scriptFileName});
            }

            // update flow file attributes
            flowFile = session.putAllAttributes(flowFile, finalScript.getAttributes());
            Relationship route = finalScript.getRoute();
            if (null == route) {
                session.remove(flowFile);
                getLogger().info("Removing flowfile {}", new Object[]{flowFile});
            } else {
                session.transfer(flowFile, route);
                getLogger().info("Transferring flowfile {} to {}", new Object[]{flowFile, route});
            }
        } catch (ScriptException | IOException e) {
            getLogger().error("Failed to create script from {} with flowFile {}. Rolling back session.",
                    new Object[]{scriptFileName, flowFile}, e);
            throw new ProcessException(e);
        } catch (Exception e) {
            if (null != script) {
                getLogger().error("Failed to execute script from {}. Transferring flow file {} to {}",
                        new Object[]{scriptFileName, flowFile, exceptionRoute}, e);
                session.transfer(flowFile, exceptionRoute);
            } else {
                getLogger().error("Failed to execute script from {} with flowFile {}. Rolling back session",
                        new Object[]{scriptFileName, flowFile}, e);
                throw new ProcessException(e);
            }
        }
    }
}
