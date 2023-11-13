# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency, ExpressionLanguageScope

# Connection Strategies
LOCAL_DISK = "Local Disk"
REMOTE_SERVER = "Remote Chroma Server"

# Authentication Strategies
TOKEN = "Token Authentication"
BASIC_AUTH = "Basic Authentication"
NONE = "None"

# Transport Protocols
HTTP = "http"
HTTPS = "https"

CONNECTION_STRATEGY = PropertyDescriptor(
    name="Connection Strategy",
    description="Specifies how to connect to the Chroma server",
    allowable_values=[LOCAL_DISK, REMOTE_SERVER],
    default_value=REMOTE_SERVER,
    required=True
)
DIRECTORY = PropertyDescriptor(
    name="Directory",
    description="The Directory that Chroma should use to persist data",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    default_value="./chroma",
    dependencies=[PropertyDependency(CONNECTION_STRATEGY, LOCAL_DISK)]
)
HOSTNAME = PropertyDescriptor(
    name="Hostname",
    description="The hostname to connect to in order to communicate with Chroma",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    default_value="localhost",
    required=True,
    dependencies=[PropertyDependency(CONNECTION_STRATEGY, REMOTE_SERVER)]
)
PORT = PropertyDescriptor(
    name="Port",
    description="The port that the Chroma server is listening on",
    validators=[StandardValidators.PORT_VALIDATOR],
    default_value="8000",
    required=True,
    dependencies=[PropertyDependency(CONNECTION_STRATEGY, REMOTE_SERVER)]
)
TRANSPORT_PROTOCOL = PropertyDescriptor(
    name="Transport Protocol",
    description="Specifies whether connections should be made over http or https",
    allowable_values=[HTTP, HTTPS],
    default_value=HTTPS,
    required=True,
    dependencies=[PropertyDependency(CONNECTION_STRATEGY, REMOTE_SERVER)]
)
AUTH_STRATEGY = PropertyDescriptor(
    name="Authentication Strategy",
    description="Specifies how to authenticate to Chroma server",
    allowable_values=[TOKEN, BASIC_AUTH, NONE],
    default_value=TOKEN,
    required=True,
    dependencies=[PropertyDependency(CONNECTION_STRATEGY, REMOTE_SERVER)]
)
AUTH_TOKEN = PropertyDescriptor(
    name="Authentication Token",
    description="The token to use for authenticating to Chroma server",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    sensitive=True,
    dependencies=[PropertyDependency(AUTH_STRATEGY, TOKEN)]
)
USERNAME = PropertyDescriptor(
    name="Username",
    description="The username to use for authenticating to Chroma server",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    dependencies=[PropertyDependency(AUTH_STRATEGY, BASIC_AUTH)]
)
PASSWORD = PropertyDescriptor(
    name="Password",
    description="The password to use for authenticating to Chroma server",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    sensitive=True,
    dependencies=[PropertyDependency(AUTH_STRATEGY, BASIC_AUTH)]
)
COLLECTION_NAME = PropertyDescriptor(
    name="Collection Name",
    description="The name of the Chroma Collection",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    default_value="nifi",
    expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)

PROPERTIES = [
    CONNECTION_STRATEGY,
    DIRECTORY,
    HOSTNAME,
    PORT,
    TRANSPORT_PROTOCOL,
    AUTH_STRATEGY,
    AUTH_TOKEN,
    USERNAME,
    PASSWORD,
    COLLECTION_NAME
]


def create_client(context):
    import chromadb
    from chromadb import Settings

    connection_strategy = context.getProperty(CONNECTION_STRATEGY).getValue()
    if connection_strategy == LOCAL_DISK:
        directory = context.getProperty(DIRECTORY).getValue()
        return chromadb.PersistentClient(directory)
    else:
        hostname = context.getProperty(HOSTNAME).getValue()
        port = context.getProperty(PORT).asInteger()
        headers = {}
        ssl = context.getProperty(TRANSPORT_PROTOCOL).getValue() == HTTPS

        auth_strategy = context.getProperty(AUTH_STRATEGY).getValue()
        if auth_strategy == TOKEN:
            auth_provider = "chromadb.auth.token.TokenAuthClientProvider"
            credentials = context.getProperty(AUTH_TOKEN).getValue()
        elif auth_strategy == BASIC_AUTH:
            auth_provider = "chromadb.auth.basic.BasicAuthClientProvider"
            username = context.getProperty(USERNAME).getValue()
            password = context.getProperty(PASSWORD).getValue()
            credentials = username + ":" + password
        else:
            auth_provider = None
            credentials = None

        settings = Settings(
            chroma_client_auth_provider=auth_provider,
            chroma_client_auth_credentials=credentials
        )
        return chromadb.HttpClient(hostname, port, ssl, headers, settings)
