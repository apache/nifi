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
package org.apache.nifi.processors.xmlcalabash;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;

import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

import com.xmlcalabash.util.Input;
import com.xmlcalabash.core.XProcConfiguration;
import com.xmlcalabash.core.XProcException;
import com.xmlcalabash.core.XProcRuntime;
import com.xmlcalabash.runtime.XPipeline;

class PipelineXmlData {
    static private final Processor saxon = new Processor(false);
    static private final XProcConfiguration config = new XProcConfiguration(saxon);
    public XProcRuntime runtime = null;
    public XPipeline pipeline = null;

    public PipelineXmlData(String pipelineData, URI baseURI) throws SaxonApiException, XProcException {
        InputStream stream = new ByteArrayInputStream(pipelineData.getBytes());
        DocumentBuilder builder = saxon.newDocumentBuilder();
        if (baseURI != null) {
            builder.setBaseURI(baseURI);
        }
        XdmNode pipelineNode = builder.build(new SAXSource(new InputSource(stream)));

        runtime = new XProcRuntime(config);
        pipeline = runtime.use(pipelineNode);
    }

    public PipelineXmlData(Input pipelineFile) throws SaxonApiException, XProcException {
        runtime = new XProcRuntime(config);
        pipeline = runtime.load(pipelineFile);
    }

}
