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
package org.apache.nifi.processors.script.impl;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.script.ScriptEngineConfigurator;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import scala.tools.nsc.Settings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.settings.MutableSettings;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

public class ScalaScriptEngineConfigurator implements ScriptEngineConfigurator, Compilable {

    private static final String PRELOADS =
            "import org.apache.nifi.components._\n"
                    + "import org.apache.nifi.flowfile.FlowFile\n"
                    + "import org.apache.nifi.processor._\n"
                    + "import org.apache.nifi.processor.exception._\n"
                    + "import org.apache.nifi.processor.io._\n"
                    + "import org.apache.nifi.processor.util._\n"
                    + "import org.apache.nifi.processors.script._\n"
                    + "import org.apache.nifi.logging.ProcessorLog\n";

    private ScriptEngine scriptEngine;

    @Override
    public String getScriptEngineName() {
        return "Scala";
    }

    @Override
    public Object init(ScriptEngine engine, String modulePath) throws ScriptException {

        if (modulePath != null) {
            final Settings settings = ((IMain) engine).settings();

            // We need to find our NAR's working lib/ directory, that's where the Scala JARs are
            String myPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();

            String parent = new File(myPath).getParent();
            String scalaLib = StringUtils.replace(parent, "\\", "/")+"/*";

            // The usual way to set up the scala compiler is to set java.class.path and then do something like this:
            //  final MutableSettings.BooleanSetting useJavaClasspath = (MutableSettings.BooleanSetting) settings.usejavacp();
            //  useJavaClasspath.tryToSetFromPropertyValue("true");
            //
            // However it appears if you set it up to use the java classpath, it won't find the rest of the stuff we
            // need (namely the NAR's dependencies.  So instead we leave the above setting "false" and build the
            // classpath ourselves, prepending the NAR's dependency folder to the value of java.class.path
            final MutableSettings.PathSetting classpath = settings.classpath();
            classpath.prepend(System.getProperty("java.class.path"));
            classpath.prepend(scalaLib);


        }
        scriptEngine = engine;
        return scriptEngine;
    }

    @Override
    public Object eval(ScriptEngine engine, InputStream scriptStream, String modulePath) throws ScriptException {
        try {
            scriptEngine = engine;
            ByteArrayInputStream bais = new ByteArrayInputStream(PRELOADS.getBytes("UTF-8"));
            SequenceInputStream fullInputStream = new SequenceInputStream(bais, scriptStream);
            return engine.eval(new InputStreamReader(fullInputStream));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            // TODO
        }

        return null;
    }

    @Override
    public CompiledScript compile(String script) throws ScriptException {
        return this.compile(new StringReader(script));
    }

    @Override
    public CompiledScript compile(Reader script) throws ScriptException {
        if (scriptEngine == null) {
            throw new ScriptException("No script engine defined!");
        }
        if (!(scriptEngine instanceof Compilable)) {
            throw new ScriptException("Script engine is not compilable!");
        }

        return ((Compilable) scriptEngine).compile(getSequenceStreamReader(new ReaderInputStream(script) ));
    }

    private Reader getSequenceStreamReader(InputStream scriptBodyStream) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(PRELOADS.getBytes("UTF-8"));
            SequenceInputStream fullInputStream = new SequenceInputStream(bais, scriptBodyStream);
            return new InputStreamReader(fullInputStream);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            // TODO
        }
        return null;
    }

}
