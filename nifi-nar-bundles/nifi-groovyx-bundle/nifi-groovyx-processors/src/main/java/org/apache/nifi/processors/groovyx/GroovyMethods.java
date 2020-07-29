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
package org.apache.nifi.processors.groovyx;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.GroovySystem;

import org.apache.nifi.processors.groovyx.flow.ProcessSessionWrap;
import org.apache.nifi.processors.groovyx.flow.SessionFile;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;
import java.util.List;

/**
 * Class to initialize additional groovy methods to work with SessionFile, Relationship, and Sessions easier
 */
class GroovyMethods {
    private static boolean initialized = false;

    static void init() {
        if (!initialized) {
            synchronized (GroovyMethods.class) {
                if (!initialized) {
                    initialized = metaRelationship();
                }
            }
        }
    }

    private static boolean metaRelationship() {
        GroovySystem.getMetaClassRegistry().setMetaClass(Relationship.class, new DelegatingMetaClass(Relationship.class) {
            @Override
            public Object invokeMethod(Object object, String methodName, Object[] args) {
                if (object instanceof Relationship) {
                    if ("leftShift".equals(methodName) && args.length == 1) {
                        if (args[0] instanceof SessionFile) {
                            return this.leftShift((Relationship) object, (SessionFile) args[0]);
                        } else if (args[0] instanceof Collection) {
                            return this.leftShift((Relationship) object, (Collection) args[0]);
                        }
                    }
                }
                return super.invokeMethod(object, methodName, args);
            }

            /** to support: REL_SUCCESS << sessionFile */
            private Relationship leftShift(Relationship r, SessionFile f) {
                f.transfer(r);
                return r;
            }

            /** to support: REL_SUCCESS << sessionFileCollection */
            @SuppressWarnings("unchecked")
            private Relationship leftShift(Relationship r, Collection sfl) {
                if (sfl != null && sfl.size() > 0) {
                    ProcessSessionWrap session = ((SessionFile) sfl.iterator().next()).session();
                    List<FlowFile> ffl = session.unwrap(sfl);
                    //assume all files has the same session
                    session.transfer(ffl, r);
                }
                return r;
            }

        });
        return true;
    }

}
