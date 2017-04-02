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
package org.apache.nifi.processors.groovyx.flow;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.io.InputStreamCallback;

import groovy.lang.Writable;
import groovy.lang.Closure;
import groovy.lang.MetaClass;
import groovy.lang.GroovyObject;
import org.codehaus.groovy.runtime.InvokerHelper;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * SessionFile with groovy specific methods.
 */
@SuppressWarnings("unused")
public class GroovySessionFile extends SessionFile implements GroovyObject {
    private transient MetaClass metaClass;

    protected GroovySessionFile(ProcessSessionWrap session, FlowFile f) {
        super(session, f);
        setMetaClass(null); //set default meta-class
    }
    /*----------------------GroovyObject methods >>---------------------------*/

    /**
     * alias method to getAttribute that will act in groovy as a property except for `size` and `attributes`
     */
    @Override
    public Object getProperty(String key) {
        if ("size".equals(key)) return getSize();
        if ("attributes".equals(key)) return getAttributes();
        return this.getAttribute(key);
    }

    /**
     * Calls putAttribute if value defined and removeAttribute if value is null
     */
    @Override
    public void setProperty(String key, Object value) {
        if (value == null) {
            this.removeAttribute(key);
        } else if (value instanceof String) {
            this.putAttribute(key, (String) value);
        } else {
            this.putAttribute(key, value.toString());
        }
    }

    /**
     * GroovyObject support method
     */
    @Override
    public MetaClass getMetaClass() {
        return this.metaClass;
    }

    /**
     * GroovyObject support method
     */
    @Override
    public void setMetaClass(MetaClass metaClass) {
        this.metaClass = metaClass == null ? InvokerHelper.getMetaClass(this.getClass()) : metaClass;
    }

    /**
     * GroovyObject support method
     */
    @Override
    public Object invokeMethod(String name, Object args) {
        return this.metaClass.invokeMethod(this, name, args);
    }
    /*----------------------<< GroovyObject methods---------------------------*/

    /*----------------------Extended Groovy methods >>------------------------*/

    /**
     * Write flow file contents through writer with defined charset.
     *
     * @param charset charset to use for writer
     * @param c       Closure that will receive writer as a parameter to write file content
     * @return reference to self
     */
    public GroovySessionFile write(String charset, Closure c) {
        this.write(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                c.call(w);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Instantly writes into flow file contents the char sequence (string).
     *
     * @param charset charset to use for writer
     * @param c       content
     * @return reference to self
     */
    public GroovySessionFile write(String charset, CharSequence c) {
        this.write(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                w.append(c);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Write flow file contents through writer with defined charset.
     *
     * @param charset charset to use for writer
     * @param c       content defined as writable
     * @return reference to self
     */
    public GroovySessionFile write(String charset, Writable c) {
        this.write(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                c.writeTo(w);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Write or read+write flow file contents through streams.
     *
     * @param c Closure that could receive one parameter OutputStream to perform write,
     *          or two parameters InputStream and OutputStream to perform read and write.
     * @return reference to self
     */
    public GroovySessionFile write(Closure c) {
        if (c.getMaximumNumberOfParameters() == 1) {
            this.write(new OutputStreamCallback() {
                public void process(OutputStream out) throws IOException {
                    c.call(out);
                }
            });
        } else {
            this.write(new StreamCallback() {
                public void process(InputStream in, OutputStream out) throws IOException {
                    c.call(in, out);
                }
            });
        }
        return this;
    }

    /**
     * Append the existing content of the flow file.
     *
     * @param c Closure that receives one parameter OutputStream to perform append.
     * @return reference to self
     */
    public GroovySessionFile append(Closure c) {
        this.append(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                c.call(out);
            }
        });
        return this;
    }

    /**
     * Append the existing content of the flow file through Writer with defined charset.
     *
     * @param charset charset to use for writer
     * @param c       content to append.
     * @return reference to self
     */
    public GroovySessionFile append(String charset, Writable c) {
        this.append(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                c.writeTo(w);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Append the existing content of the flow file through Writer with defined charset.
     *
     * @param charset charset to use for writer
     * @param c       Closure with one parameter - Writer.
     * @return reference to self
     */
    public GroovySessionFile append(String charset, Closure c) {
        this.append(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                c.call(w);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Append the existing content of the flow file through Writer with defined charset.
     *
     * @param charset charset to use for writer
     * @param c       content to append.
     * @return reference to self
     */
    public GroovySessionFile append(String charset, CharSequence c) {
        this.append(new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
                Writer w = new OutputStreamWriter(out, charset);
                w.append(c);
                w.flush();
                w.close();
            }
        });
        return this;
    }

    /**
     * Reads content of the flow file and closes input stream.
     *
     * @param c Closure with one parameter InputStream.
     */
    public void read(Closure c) {
        this.read(new InputStreamCallback() {
            public void process(InputStream in) throws IOException {
                c.call(in);
            }
        });
    }

    /**
     * Reads content of the flow file through Reader and closes the stream.
     *
     * @param charset charset to use for Reader
     * @param c       Closure with one parameter Reader.
     */
    public void read(String charset, Closure c) {
        this.read(new InputStreamCallback() {
            public void process(InputStream in) throws IOException {
                InputStreamReader r = new InputStreamReader(in, charset);
                c.call(r);
                r.close();
            }
        });
    }

}
