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

import groovy.lang.Closure;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;

import java.util.List;

/**
 * Wrapped session that produces groovy wrapped session-file.
 */
@SuppressWarnings("unused")
public class GroovyProcessSessionWrap extends ProcessSessionWrap {

    public GroovyProcessSessionWrap(ProcessSession s, boolean toFailureOnError) {
        super(s, toFailureOnError);
    }

    /**
     * function returns wrapped flow file with session for the simplified script access.
     */
    public SessionFile wrap(FlowFile f) {
        if (f == null) {
            return null;
        }
        if (f instanceof SessionFile) {
            return ((SessionFile) f);
        }
        return new GroovySessionFile(this, f);
    }

    /**
     * returns filtered list of input files. the closure receives each file from input queue and should return one of values:
     * true - accept and continue, false - reject and continue, null - reject and stop, or any FlowFileFilterResult value.
     */
    public List<FlowFile> get(Closure filter) {
        return this.get(new FlowFileFilter() {
            @SuppressWarnings("ConstantConditions")
            public FlowFileFilterResult filter(FlowFile flowFile) {
                Object res = filter.call(wrap(flowFile));
                if (res == null) {
                    return FlowFileFilterResult.REJECT_AND_TERMINATE;
                }
                if (res instanceof Boolean) {
                    return ((Boolean) res ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE);
                }
                if (res instanceof FlowFileFilterResult) {
                    return (FlowFileFilterResult) res;
                }
                return (org.codehaus.groovy.runtime.DefaultGroovyMethods.asBoolean(res) ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE);
            }
        });
    }
}
