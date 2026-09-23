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
package org.apache.nifi.services.protobuf;

import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.GroupElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.List;

final class ProtobufCustomOptionRemover {

    private ProtobufCustomOptionRemover() {
    }

    /**
     * Removes custom (parenthesized) Protocol Buffer options from schema text.
     * Built-in options such as {@code packed} and {@code java_package} are retained.
     * When the text cannot be parsed, the original string is returned.
     *
     * @param schemaText source proto schema to remove custom options from
     * @param location   source location used for parse diagnostics and regenerated comments
     * @param logger     component logger
     * @return schema text without custom options, or the original text when parsing fails or no custom options are present
     */
    static String remove(final String schemaText, final Location location, final ComponentLog logger) {
        if (schemaText == null || schemaText.isBlank()) {
            return schemaText;
        }

        final ProtoFileElement parsed;
        try {
            parsed = ProtoParser.Companion.parse(location, schemaText);
        } catch (final IllegalStateException e) {
            logger.warn("Failed to parse Protobuf schema [{}] while removing custom options; using the original schema text", location, e);
            return schemaText;
        }

        final ProtoFileElement withoutCustomOptions = removeFromFile(parsed);
        if (parsed.equals(withoutCustomOptions)) {
            return schemaText;
        }

        return withoutCustomOptions.toSchema();
    }

    private static ProtoFileElement removeFromFile(final ProtoFileElement file) {
        return new ProtoFileElement(
            file.getLocation(),
            file.getPackageName(),
            file.getSyntax(),
            file.getImports(),
            file.getPublicImports(),
            file.getWeakImports(),
            removeFromTypes(file.getTypes()),
            removeFromServices(file.getServices()),
            removeFromExtendDeclarations(file.getExtendDeclarations()),
            removeCustomOptions(file.getOptions())
        );
    }

    private static List<TypeElement> removeFromTypes(final List<TypeElement> types) {
        final List<TypeElement> updatedTypes = new ArrayList<>(types.size());
        for (final TypeElement type : types) {
            updatedTypes.add(removeFromType(type));
        }

        return updatedTypes;
    }

    private static TypeElement removeFromType(final TypeElement type) {
        if (type instanceof MessageElement message) {
            return new MessageElement(
                message.getLocation(),
                message.getName(),
                message.getDocumentation(),
                removeFromTypes(message.getNestedTypes()),
                removeCustomOptions(message.getOptions()),
                message.getReserveds(),
                removeFromFields(message.getFields()),
                removeFromOneOfs(message.getOneOfs()),
                removeFromExtensions(message.getExtensions()),
                removeFromGroups(message.getGroups()),
                removeFromExtendDeclarations(message.getExtendDeclarations())
            );
        }

        if (type instanceof EnumElement enumeration) {
            return new EnumElement(
                enumeration.getLocation(),
                enumeration.getName(),
                enumeration.getDocumentation(),
                removeCustomOptions(enumeration.getOptions()),
                removeFromEnumConstants(enumeration.getConstants()),
                enumeration.getReserveds()
            );
        }

        return type;
    }

    private static List<FieldElement> removeFromFields(final List<FieldElement> fields) {
        final List<FieldElement> updatedFields = new ArrayList<>(fields.size());
        for (final FieldElement field : fields) {
            updatedFields.add(new FieldElement(
                field.getLocation(),
                field.getLabel(),
                field.getType(),
                field.getName(),
                field.getDefaultValue(),
                field.getJsonName(),
                field.getTag(),
                field.getDocumentation(),
                removeCustomOptions(field.getOptions())
            ));
        }

        return updatedFields;
    }

    private static List<OneOfElement> removeFromOneOfs(final List<OneOfElement> oneOfs) {
        final List<OneOfElement> updatedOneOfs = new ArrayList<>(oneOfs.size());
        for (final OneOfElement oneOf : oneOfs) {
            updatedOneOfs.add(new OneOfElement(
                oneOf.getName(),
                oneOf.getDocumentation(),
                removeFromFields(oneOf.getFields()),
                removeFromGroups(oneOf.getGroups()),
                removeCustomOptions(oneOf.getOptions()),
                oneOf.getLocation()
            ));
        }

        return updatedOneOfs;
    }

    private static List<GroupElement> removeFromGroups(final List<GroupElement> groups) {
        final List<GroupElement> updatedGroups = new ArrayList<>(groups.size());
        for (final GroupElement group : groups) {
            updatedGroups.add(new GroupElement(
                group.getLabel(),
                group.getLocation(),
                group.getName(),
                group.getTag(),
                group.getDocumentation(),
                removeFromFields(group.getFields())
            ));
        }

        return updatedGroups;
    }

    private static List<ExtensionsElement> removeFromExtensions(final List<ExtensionsElement> extensions) {
        final List<ExtensionsElement> updatedExtensions = new ArrayList<>(extensions.size());
        for (final ExtensionsElement extension : extensions) {
            updatedExtensions.add(new ExtensionsElement(
                extension.getLocation(),
                extension.getDocumentation(),
                extension.getValues(),
                removeCustomOptions(extension.getOptions())
            ));
        }

        return updatedExtensions;
    }

    private static List<ExtendElement> removeFromExtendDeclarations(final List<ExtendElement> extendDeclarations) {
        final List<ExtendElement> updatedDeclarations = new ArrayList<>(extendDeclarations.size());
        for (final ExtendElement extendDeclaration : extendDeclarations) {
            updatedDeclarations.add(new ExtendElement(
                extendDeclaration.getLocation(),
                extendDeclaration.getName(),
                extendDeclaration.getDocumentation(),
                removeFromFields(extendDeclaration.getFields())
            ));
        }

        return updatedDeclarations;
    }

    private static List<EnumConstantElement> removeFromEnumConstants(final List<EnumConstantElement> constants) {
        final List<EnumConstantElement> updatedConstants = new ArrayList<>(constants.size());
        for (final EnumConstantElement constant : constants) {
            updatedConstants.add(new EnumConstantElement(
                constant.getLocation(),
                constant.getName(),
                constant.getTag(),
                constant.getDocumentation(),
                removeCustomOptions(constant.getOptions())
            ));
        }

        return updatedConstants;
    }

    private static List<ServiceElement> removeFromServices(final List<ServiceElement> services) {
        final List<ServiceElement> updatedServices = new ArrayList<>(services.size());
        for (final ServiceElement service : services) {
            updatedServices.add(new ServiceElement(
                service.getLocation(),
                service.getName(),
                service.getDocumentation(),
                removeFromRpcs(service.getRpcs()),
                removeCustomOptions(service.getOptions())
            ));
        }

        return updatedServices;
    }

    private static List<RpcElement> removeFromRpcs(final List<RpcElement> rpcs) {
        final List<RpcElement> updatedRpcs = new ArrayList<>(rpcs.size());
        for (final RpcElement rpc : rpcs) {
            updatedRpcs.add(new RpcElement(
                rpc.getLocation(),
                rpc.getName(),
                rpc.getDocumentation(),
                rpc.getRequestType(),
                rpc.getResponseType(),
                rpc.getRequestStreaming(),
                rpc.getResponseStreaming(),
                removeCustomOptions(rpc.getOptions())
            ));
        }

        return updatedRpcs;
    }

    private static List<OptionElement> removeCustomOptions(final List<OptionElement> options) {
        final List<OptionElement> retainedOptions = new ArrayList<>(options.size());
        for (final OptionElement option : options) {
            // Custom options are always parenthesized. Built-in options are not.
            if (!option.isParenthesized()) {
                retainedOptions.add(option);
            }
        }

        return retainedOptions;
    }
}
