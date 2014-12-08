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
package org.apache.nifi.cluster.protocol;

import org.apache.nifi.cluster.protocol.DataFlow;
import java.io.Serializable;
import java.util.Arrays;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.jaxb.message.DataFlowAdapter;

/**
 * Represents a dataflow, which includes the raw bytes of the flow.xml and 
 * whether processors should be started automatically at application startup.
 */
@XmlJavaTypeAdapter(DataFlowAdapter.class)
public class StandardDataFlow implements Serializable, DataFlow {
    
    private final byte[] flow;
    private final byte[] templateBytes;
    private final byte[] snippetBytes;

    private boolean autoStartProcessors;
    
    /**
     * Constructs an instance.  
     * 
     * @param flow a valid flow as bytes, which cannot be null
     * @param templateBytes an XML representation of templates
     * @param snippetBytes an XML representation of snippets
     * 
     * @throws NullPointerException if any argument is null
     */
    public StandardDataFlow(final byte[] flow, final byte[] templateBytes, final byte[] snippetBytes) {
        this.flow = flow;
        this.templateBytes = templateBytes;
        this.snippetBytes = snippetBytes;
    }
    
    public StandardDataFlow(final DataFlow toCopy) {
        this.flow = copy(toCopy.getFlow());
        this.templateBytes = copy(toCopy.getTemplates());
        this.snippetBytes = copy(toCopy.getSnippets());
        this.autoStartProcessors = toCopy.isAutoStartProcessors();
    }
    
    private static byte[] copy(final byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
    
    /**
     * @return the raw byte array of the flow 
     */
    public byte[] getFlow() {
        return flow;
    }

    /**
     * @return the raw byte array of the templates
     */
    public byte[] getTemplates() {
        return templateBytes;
    }
    
    /**
     * @return the raw byte array of the snippets
     */
    public byte[] getSnippets() {
        return snippetBytes;
    }
    
    /**
     * @return true if processors should be automatically started at application 
     * startup; false otherwise 
     */
    public boolean isAutoStartProcessors() {
        return autoStartProcessors;
    }
    
    /**
     * 
     * Sets the flag to automatically start processors at application startup.
     * 
     * @param autoStartProcessors true if processors should be automatically
     * started at application startup; false otherwise
     */
    public void setAutoStartProcessors(final boolean autoStartProcessors) {
        this.autoStartProcessors = autoStartProcessors;
    }
}
