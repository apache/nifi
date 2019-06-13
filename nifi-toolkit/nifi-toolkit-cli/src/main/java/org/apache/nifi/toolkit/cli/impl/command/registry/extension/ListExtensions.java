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
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadataContainer;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.ExtensionMetadataResult;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class ListExtensions extends AbstractNiFiRegistryCommand<ExtensionMetadataResult> {

    public ListExtensions() {
        super("list-extensions", ExtensionMetadataResult.class);
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.EXT_TAGS.createOption());
        addOption(CommandOption.EXT_TYPE.createOption());
    }

    @Override
    public String getDescription() {
        return "Lists info for extensions, optionally filtering by one or more tags. If specifying tags, multiple tags can " +
                "be specified with a comma-separated list, and each tag will be OR'd together.";
    }

    @Override
    public ExtensionMetadataResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String tags = getArg(properties, CommandOption.EXT_TAGS);
        final String extensionType = getArg(properties, CommandOption.EXT_TYPE);

        final ExtensionClient extensionClient = client.getExtensionClient();
        final ExtensionFilterParams filterParams = getFilterParams(tags, extensionType);
        final ExtensionMetadataContainer metadataContainer = extensionClient.findExtensions(filterParams);

        final List<ExtensionMetadata> metadataList = new ArrayList<>(metadataContainer.getExtensions());
        return new ExtensionMetadataResult(getResultType(properties), metadataList);
    }

    // NOTE: There is a bug in the nifi-registry-client that sends bundleType from filter params using the name() instead of toString().
    // When that is resolved we can update this command to have an optional argument for bundle type and set it in the filter params.

    private ExtensionFilterParams getFilterParams(final String tagsArg, final String extensionTypeArg) throws NiFiRegistryException {
        final ExtensionFilterParams.Builder builder = new ExtensionFilterParams.Builder();

        if (!StringUtils.isBlank(tagsArg)) {
            final String[] splitTags = tagsArg.split("[,]");

            final Set<String> cleanedTags = Arrays.stream(splitTags)
                    .map(t -> t.trim())
                    .collect(Collectors.toSet());

            if (cleanedTags.isEmpty()) {
                throw new IllegalArgumentException("Invalid tag argument");
            }

            builder.addTags(cleanedTags).build();
        }

        if (!StringUtils.isEmpty(extensionTypeArg)) {
            try {
                final ExtensionType extensionType = ExtensionType.valueOf(extensionTypeArg);
                builder.extensionType(extensionType);
            } catch (Exception e) {
                throw new NiFiRegistryException("Invalid extension type, should be one of "
                        + ExtensionType.PROCESSOR.toString() + ", "
                        + ExtensionType.CONTROLLER_SERVICE.toString() + ", or "
                        + ExtensionType.REPORTING_TASK.toString());
            }
        }

        return builder.build();
    }
}
