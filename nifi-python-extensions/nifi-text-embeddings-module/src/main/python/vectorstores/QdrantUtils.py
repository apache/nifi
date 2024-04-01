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

from nifiapi.properties import (
    PropertyDescriptor,
    StandardValidators,
    ExpressionLanguageScope,
    PropertyDependency,
)
from EmbeddingUtils import (
    OPENAI,
    HUGGING_FACE,
    EMBEDDING_MODEL,
)

import uuid

DEFAULT_COLLECTION_NAME = "apache-nifi"


COLLECTION_NAME = PropertyDescriptor(
    name="Collection Name",
    description="The name of the Qdrant collection to use.",
    sensitive=False,
    required=True,
    default_value=DEFAULT_COLLECTION_NAME,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
)
QDRANT_URL = PropertyDescriptor(
    name="Qdrant URL",
    description="The fully qualified URL to the Qdrant instance.",
    sensitive=False,
    required=True,
    default_value="http://localhost:6333",
    validators=[StandardValidators.URL_VALIDATOR],
)
QDRANT_API_KEY = PropertyDescriptor(
    name="Qdrant API Key",
    description="The API Key to use in order to authentication with Qdrant. Can be empty.",
    sensitive=True,
    required=True,
)

PREFER_GRPC = PropertyDescriptor(
    name="Prefer gRPC",
    description="Specifies whether to use gRPC for interfacing with Qdrant.",
    required=True,
    default_value=False,
    allowable_values=["True", "False"],
    validators=[StandardValidators.BOOLEAN_VALIDATOR],
)
HTTPS = PropertyDescriptor(
    name="Use HTTPS",
    description="Specifies whether to TLS(HTTPS) while interfacing with Qdrant.",
    required=True,
    default_value=False,
    allowable_values=["True", "False"],
    validators=[StandardValidators.BOOLEAN_VALIDATOR],
)

QDRANT_PROPERTIES = [COLLECTION_NAME, QDRANT_URL, QDRANT_API_KEY, PREFER_GRPC, HTTPS]

HUGGING_FACE_API_KEY = PropertyDescriptor(
    name="HuggingFace API Key",
    description="The API Key for interacting with HuggingFace",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    sensitive=True,
    dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)],
)
HUGGING_FACE_MODEL = PropertyDescriptor(
    name="HuggingFace Model",
    description="The name of the HuggingFace model to use.",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    default_value="sentence-transformers/all-MiniLM-L6-v2",
    dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)],
)
OPENAI_API_KEY = PropertyDescriptor(
    name="OpenAI API Key",
    description="The API Key for OpenAI in order to create embeddings.",
    sensitive=True,
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)],
)
OPENAI_API_MODEL = PropertyDescriptor(
    name="OpenAI Model",
    description="The name of the OpenAI model to use.",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    default_value="text-embedding-ada-002",
    dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)],
)

EMBEDDING_MODEL_PROPERTIES = [
    EMBEDDING_MODEL,
    HUGGING_FACE_API_KEY,
    HUGGING_FACE_MODEL,
    OPENAI_API_KEY,
    OPENAI_API_MODEL,
]


def convert_id(_id: str) -> str:
    """
    Converts any string into a UUID string deterministically.

    Qdrant accepts UUID strings and unsigned integers as point ID.
    This allows us to overwrite the same point with the original ID.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, _id))
