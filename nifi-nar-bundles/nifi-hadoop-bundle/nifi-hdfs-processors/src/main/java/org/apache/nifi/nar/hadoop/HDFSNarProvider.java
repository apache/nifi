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
package org.apache.nifi.nar.hadoop;

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.flow.resource.FlowResourceDescriptor;
import org.apache.nifi.nar.NarProvider;
import org.apache.nifi.flow.resource.hadoop.HDFSFlowResourceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.stream.Collectors;

@Deprecated
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public class HDFSNarProvider extends HDFSFlowResourceProvider implements NarProvider {

    @Override
    public Collection<String> listNars() throws IOException {
        return listResources().stream().map(FlowResourceDescriptor::getFileName).collect(Collectors.toSet());
    }

    @Override
    public InputStream fetchNarContents(final String location) throws IOException {
        return fetchExternalResource(location);
    }
}
