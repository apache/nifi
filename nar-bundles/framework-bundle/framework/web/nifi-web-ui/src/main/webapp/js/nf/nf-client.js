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
nf.Client = {};

nf.Client.version = -1;
nf.Client.clientId = null;

/**
 * Gets the current revision.
 */
nf.Client.getRevision = function () {
    return {
        version: nf.Client.version,
        clientId: nf.Client.clientId
    };
};

/**
 * Sets the current revision.
 * 
 * @argument {integer} revision     The revision
 */
nf.Client.setRevision = function (revision) {
    // ensure a value was returned
    if (nf.Common.isDefinedAndNotNull(revision.version)) {
        if (nf.Common.isDefinedAndNotNull(nf.Client.version)) {
            // if the client version was already set, ensure
            // the new value is greater
            if (revision.version > nf.Client.version) {
                nf.Client.version = revision.version;
            }
        } else {
            // otherwise just set the value
            nf.Client.version = revision.version;
        }
    }

    // ensure a value was returned
    if (nf.Common.isDefinedAndNotNull(revision.clientId)) {
        nf.Client.clientId = revision.clientId;
    }
};