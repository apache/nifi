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
package org.apache.nifi.toolkit.cli.impl.command.registry.extension;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.ExtensionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.TagCountResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ListExtensionTags extends AbstractNiFiRegistryCommand<TagCountResult> {

    public ListExtensionTags() {
        super("list-extension-tags", TagCountResult.class);
    }

    @Override
    public String getDescription() {
        return "Lists the tag counts for all extensions located in buckets the current user is authorized for.";
    }

    @Override
    public TagCountResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {
        final ExtensionClient extensionClient = client.getExtensionClient();
        final List<TagCount> tagCounts = extensionClient.getTagCounts();
        return new TagCountResult(getResultType(properties), tagCounts);
    }
}
