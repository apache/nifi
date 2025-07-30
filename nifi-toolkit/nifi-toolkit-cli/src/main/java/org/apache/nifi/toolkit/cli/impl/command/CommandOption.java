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
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.commons.cli.Option;

/**
 * All possible options for commands.
 */
public enum CommandOption {

    // General
    CONNECTION_TIMEOUT("cto", "connectionTimeout", "Timeout parameter for creating a connection to NiFi/Registry, specified in milliseconds", true),
    READ_TIMEOUT("rto", "readTimeout", "Timeout parameter for reading from NiFi/Registry, specified in milliseconds", true),
    URL("u", "baseUrl", "The URL to execute the command against", true),
    INPUT_SOURCE("i", "input", "A local file to read as input contents, a directory to read files from or a public URL to fetch", true, true),
    OUTPUT_FILE("o", "outputFile", "A file to write output to, must contain full path and filename", true, true),
    OUTPUT_DIR("od", "outputDirectory", "A directory to write output to", true, true),
    PROPERTIES("p", "properties", "A properties file to load arguments from, " +
            "command line values will override anything in the properties file, must contain full path to file", true, true),
    FILE_EXTENSION("fe", "fileExtension", "A file extension such as '.nar'", true, false),

    NIFI_PROPS("nifiProps", "nifiProps", "A properties file to load for NiFi config", true, true),
    NIFI_REG_PROPS("nifiRegProps", "nifiRegProps", "A properties file to load for NiFi Registry config", true, true),

    // Registry - Buckets
    BUCKET_ID("b", "bucketIdentifier", "A bucket identifier", true),
    BUCKET_NAME("bn", "bucketName", "A bucket name", true),
    BUCKET_DESC("bd", "bucketDesc", "A bucket description", true),

    // Registry - Flows
    FLOW_ID("f", "flowIdentifier", "A flow identifier", true),
    FLOW_NAME("fn", "flowName", "A flow name", true),
    FLOW_DESC("fd", "flowDesc", "A flow description", true),
    FLOW_VERSION("fv", "flowVersion", "A version of a flow", true),
    FLOW_BRANCH("fb", "flowBranch", "A branch for the flow", true),

    FLOW_VERSION_1("fv1", "flowVersion1", "A version of a flow", true),
    FLOW_VERSION_2("fv2", "flowVersion2", "A version of a flow", true),

    // Registry - Source options for when there are two registries involved and one
    // is a source
    SRC_PROPS("sp", "sourceProps", "A properties file to load for the source", true, true),
    SRC_FLOW_ID("sf", "sourceFlowIdentifier", "A flow identifier from the source registry", true),
    SRC_FLOW_VERSION("sfv", "sourceFlowVersion", "A version of a flow from the source registry", true),

    // Registry - Extensions
    EXT_BUNDLE_GROUP("gr", "group", "The group id of a bundle", true),
    EXT_BUNDLE_ARTIFACT("ar", "artifact", "The artifact id of a bundle", true),
    EXT_BUNDLE_VERSION("ver", "version", "The version of the bundle", true),
    EXT_BUNDLE_CURRENT_VERSION("cver", "current-version", "The current version of the bundle", true),
    EXT_QUALIFIED_NAME("extname", "extension-name", "The qualified name of the extension", true),

    EXT_TYPE("et", "extensionType", "The type of extension, one of 'PROCESSOR', 'CONTROLLER_SERVICE', or 'REPORTING_TASK'.", true),
    EXT_BUNDLE_TYPE("ebt", "extensionBundleType", "The type of extension bundle, either nifi-nar or minifi-cpp", true),
    EXT_BUNDLE_FILE("ebf", "extensionBundleFile", "An extension bundle file, such as a NAR or MiNiFi CPP binary", true, true),
    EXT_BUNDLE_DIR("ebd", "extensionBundleDir", "A directory where extension bundles are located", true, true),
    SKIP_SHA_256("skipSha256", "skipSha256", "Skips the client side calculation of the SHA-256 when uploading an extension bundle", false),

    EXT_TAGS("tags", "tags", "A comma separated list of one or more extension tags", true),

    // NiFi - Nodes
    NIFI_NODE_ID("nnid", "nifiNodeId", "The ID of a node in the NiFi cluster", true),

    // NiFi - Registries
    REGISTRY_CLIENT_ID("rcid", "registryClientId", "The id of a registry client", true),
    REGISTRY_CLIENT_NAME("rcn", "registryClientName", "The name of the registry client", true),
    REGISTRY_CLIENT_URL("rcu", "registryClientUrl", "The url of the registry client", true),
    REGISTRY_CLIENT_DESC("rcd", "registryClientDesc", "The description of the registry client", true),
    REGISTRY_CLIENT_TYPE("rct", "registryClientType", "The type of the registry client", true),
    SSL_CONTEXT_SERVICE_ID("ssl", "sslContextServiceId", "The ID of SSL Context Service", true),
    
    // NiFi - PGs
    PG_ID("pgid", "processGroupId", "The id of a process group", true),
    PG_NAME("pgn", "processGroupName", "The name of a process group", true),
    PG_VAR_NAME("var", "varName", "The name of a variable", true),
    PG_VAR_VALUE("val", "varValue", "The value of a variable", true),
    KEEP_EXISTING_PARAMETER_CONTEXT("kepc", "keep-existing-parameter-context", "If false, only directly associated Parameter Contexts will be copied, "
            + "inherited Contexts with no direct assignment to a Process Group are ignored", true),
    INCLUDE_REFERENCED_SERVICES("irs", "include-referenced-services", "Indicates that " +
            "referenced services from outside the target group will be included.", false),

    POS_X("px", "posX", "The x coordinate of a position", true),
    POS_Y("py", "posY", "The y coordinate of a position", true),

    SOURCE_PG("sourcePg", "source-pg", "The ID of the source process group", true),
    DESTINATION_PG("destPg", "destination-pg", "The ID of the destination process group", true),
    SOURCE_OUTPUT_PORT("sourceOutput", "source-output-port", "The name of the output port in the source process group", true),
    DESTINATION_INPUT_PORT("destInput", "destination-input-port", "The name of the input port in the destination process group", true),

    // NiFi - Processors
    PROC_ID("procid", "processorId", "The id of a processor", true),

    // NiFi - Controller Services
    CS_ID("cs", "controllerServiceId", "The id of a controller service", true),

    // NiFi - Reporting Tasks
    RT_ID("rt", "reportingTaskId", "The id of a reporting task", true),

    // NiFi - Flow Analysis Rules
    FAR_ID("far", "flowAnalysisRuleId", "The id of a flow analysis rule", true),

    // NiFi - User/Group
    USER_NAME("un", "userName", "The name of a user", true),
    USER_ID("ui", "userIdentifier", "The identifier of a user", true),
    UG_ID("ugid", "userGroupId", "The id of a user group", true),
    UG_NAME("ugn", "userGroupName", "The name of a user group", true),
    USER_NAME_LIST("unl", "userNameList", "The comma-separated user name list", true),
    USER_ID_LIST("uil", "userIdList", "The comma-separated user id list", true),
    GROUP_NAME_LIST("gnl", "groupNameList", "The comma-separated user group name list", true),
    GROUP_ID_LIST("gil", "groupIdList", "The comma-separated user group id list", true),

    // NiFi - Access Policies
    POLICY_RESOURCE("por", "accessPolicyResource", "The resource of an access policy", true),
    POLICY_ACTION("poa", "accessPolicyAction", "The action of an access policy (read or write)", true),
    OVERWRITE_POLICY("owp", "overwritePolicy", "Overwrite the user list and group list for the access policy", false),

    // NiFI - Parameter Contexts
    PARAM_CONTEXT_ID("pcid", "paramContextId", "The id of a parameter context", true),
    PARAM_CONTEXT_NAME("pcn", "paramContextName", "The name of a parameter context", true),
    PARAM_CONTEXT_DESC("pcd", "paramContextDescription", "The description of a parameter context", true),
    PARAM_CONTEXT_INCLUDE_INHERITED("pcin", "paramContextIncludeInherited", "Indicates that all inherited parameters should be included", false),
    PARAM_CONTEXT_INHERITED_IDS("pcii", "paramContextInheritedIds", "A comma-separated list of parameter context IDs to inherit", true),

    // NiFI - Parameter Providers
    PARAM_PROVIDER_ID("ppid", "paramProviderId", "The id of a parameter provider", true),
    PARAM_PROVIDER_NAME("ppn", "paramProviderName", "The name of a parameter provider", true),
    PARAM_PROVIDER_TYPE("ppt", "paramProviderType", "The type (fully qualified class name) of a parameter provider", true),
    PARAM_PROVIDER_GROUP_ID("ppgid", "paramProviderGroupId", "The bundle group ID of a parameter provider", true),
    PARAM_PROVIDER_ARTIFACT_ID("ppaid", "paramProviderArtifactId", "The bundle artifact ID of a parameter provider", true),
    PARAM_PROVIDER_VERSION("ppv", "paramProviderVersion", "The bundle version of a parameter provider", true),
    APPLY_PARAMETERS("ap", "applyParameters", "If specified, the fetched parameters will also be applied to all referencing parameter contexts", false),
    SENSITIVE_PARAM_PATTERN("spp", "sensitiveParamPattern", "A Regular Expression indicating the names of parameters that should be fetched as Sensitive.  " +
            "If not specified, and --inputSource (-i) is not specified, all fetched parameters will be Sensitive.", true),
    PROPERTY_NAME("prna", "propertyName", "The name of a property", true),
    PROPERTY_VALUE("prva", "propertyValue", "The value of a property", true),

    PARAM_NAME("pn", "paramName", "The name of the parameter", true),
    PARAM_DESC("pd", "paramDescription", "The description of the parameter", true),
    PARAM_VALUE("pv", "paramValue", "The value of a parameter", true),
    PARAM_SENSITIVE("ps", "paramSensitive", "Whether or not the parameter is sensitive (true/false)", true),
    UPDATE_TIMEOUT("ut", "updateTimeout", "Number of seconds after which a parameter context update will timeout (default: 60, maximum: 600)", true),

    // NiFi - NARs
    NAR_ID("nid", "narId", "", true),
    NAR_FILE("nar", "narFile", "A NAR file to upload, must contain full path and filename", true, true),
    NAR_UPLOAD_TIMEOUT("npt", "narProcessing", "Number of seconds after which a parameter context update will timeout (default: 60, maximum: 600)", true),

    // NiFi - Assets
    ASSET_FILE("af", "assetFile", "A file containing the asset content, must contain full path and filename", true, true),
    ASSET_ID("aid", "assetId", "The id of an asset which can be referenced from a parameter", true, false),

    // Security related
    KEYSTORE("ks", "keystore", "A keystore to use for TLS/SSL connections", true),
    KEYSTORE_TYPE("kst", "keystoreType", "The type of key store being used such as PKCS12", true),
    KEYSTORE_PASSWORD("ksp", "keystorePasswd", "The password of the keystore being used", true),
    KEY_PASSWORD("kp", "keyPasswd", "The key password of the keystore being used", true),
    TRUSTSTORE("ts", "truststore", "A truststore to use for TLS/SSL connections", true),
    TRUSTSTORE_TYPE("tst", "truststoreType", "The type of trust store being used such as PKCS12", true),
    TRUSTSTORE_PASSWORD("tsp", "truststorePasswd", "The password of the truststore being used", true),
    PROXIED_ENTITY("pe", "proxiedEntity", "The identity of an entity to proxy", true),
    PROTOCOL("pro", "protocol", "The security protocol to use, such as TLSv.1.2", true),

    BASIC_AUTH_USER("bau", "basicAuthUsername", "The username for basic auth", true),
    BASIC_AUTH_PASSWORD("bap", "basicAuthPassword", "The password for basic auth ", true),

    BEARER_TOKEN("btk", "bearerToken", "The bearer token to be passed in the Authorization header of a request", true),

    USERNAME("usr", "username", "The username for authentication when obtaining an access token", true),
    PASSWORD("pwd", "password", "The password for authentication when obtaining an access token", true),

    OIDC_TOKEN_URL("oidctokenurl", "oidcTokenUrl", "The OIDC URL to access the token endpoint for the OAuth Client Credentials Flow", true),
    OIDC_CLIENT_ID("oidcid", "oidcClientId", "The Client ID for the OAuth Client Credentials Flow", true),
    OIDC_CLIENT_SECRET("oidcsecret", "oidcClientSecret", "The Client Secret for the OAuth Client Credentials Flow", true),

    KERBEROS_PRINCIPAL("krbPr", "kerberosPrincipal", "The kerberos principal", true),
    KERBEROS_KEYTAB("krbKt", "kerberosKeytab", "The keytab for a kerberos principal", true, true),
    KERBEROS_PASSWORD("krbPw", "kerberosPassword", "The password for a kerberos principal", true),

    // Miscellaneous
    FILTER("filter", "filter", "Indicates a filter that should be used to perform the action", true),
    FORCE("force", "force", "Indicates to force the operation", false),
    OUTPUT_TYPE("ot", "outputType", "The type of output to produce (json or simple)", true),
    VERBOSE("verbose", "verbose", "Indicates that verbose output should be provided", false),
    RECURSIVE("r", "recursive", "Indicates the command should perform the action recursively", false),
    HELP("h", "help", "Help", false),
    SKIP_EXISTING("se", "skipExisting", "Indicates to skip an operation if target object exists", false);

    private final String shortName;
    private final String longName;
    private final String description;
    private final boolean hasArg;
    private final boolean isFile;

    CommandOption(final String shortName, final String longName, final String description, final boolean hasArg) {
        this(shortName, longName, description, hasArg, false);
    }

    CommandOption(final String shortName, final String longName, final String description, final boolean hasArg, final boolean isFile) {
        this.shortName = shortName;
        this.longName = longName;
        this.description = description;
        this.hasArg = hasArg;
        this.isFile = isFile;
    }

    public String getShortName() {
        return shortName;
    }

    public String getLongName() {
        return longName;
    }

    public String getDescription() {
        return description;
    }

    public boolean isFile() {
        return isFile;
    }

    public Option createOption() {
        return Option.builder(shortName).longOpt(longName).desc(description).hasArg(hasArg).build();
    }

    public Option createOption(final String description) {
        return Option.builder(shortName).longOpt(longName).desc(description).hasArg(hasArg).build();
    }
}
