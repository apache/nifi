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

package org.apache.nifi.processors.daffodil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;

import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.UnparseResult;
import org.apache.daffodil.japi.infoset.InfosetInputter;
import org.apache.daffodil.japi.infoset.JsonInfosetInputter;
import org.apache.daffodil.japi.infoset.XMLTextInfosetInputter;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to transform an XML or JSON representation of data back to the original data format.")
@WritesAttribute(attribute = "mime.type", description = "If the FlowFile is successfully unparsed, this attriute is removed, as the MIME Type is no longer known.")
public class DaffodilUnparse extends AbstractDaffodilProcessor {

    private InfosetInputter getInfosetInputter(String infosetType, Reader rdr) {
        switch (infosetType) {
            case XML_VALUE: return new XMLTextInfosetInputter(rdr);
            case JSON_VALUE: return new JsonInfosetInputter(rdr);
            default: throw new AssertionError("Unhandled infoset type: " + infosetType);
        }
    }

    @Override
    protected boolean isUnparse() {
        return true;
    }

    /**
     * The resulting output mime type of an unparse action cannot be known
     * since it is entirely based on the DFDL schema. Since we do not know the
     * mime type, return null. This will signifiy to the abstract daffodil
     * processor that the mime.type attribute should be removed from the output
     * FlowFile.
     */
    @Override
    protected String getOutputMimeType(String infosetType) {
        return null;
    }

    @Override
    protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream in, final OutputStream out, String infosetType) throws IOException {
        InfosetInputter inputter = getInfosetInputter(infosetType, new InputStreamReader(in));
        WritableByteChannel wbc = Channels.newChannel(out);
        UnparseResult ur = dp.unparse(inputter, wbc);
        if (ur.isError()) {
            getLogger().error("Failed to unparse {}", new Object[]{ff});
            logDiagnostics(ur);
            throw new DaffodilProcessingException("Failed to unparse");
        }
    }

}

