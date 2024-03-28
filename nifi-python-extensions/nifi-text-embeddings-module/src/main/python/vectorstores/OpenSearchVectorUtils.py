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

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope, PropertyDependency
from EmbeddingUtils import OPENAI, HUGGING_FACE, EMBEDDING_MODEL

# Space types
L2 = ("L2 (Euclidean distance)", "l2")
L1 = ("L1 (Manhattan distance)", "l1")
LINF = ("L-infinity (chessboard) distance", "linf")
COSINESIMIL = ("Cosine similarity", "cosinesimil")

HUGGING_FACE_API_KEY = PropertyDescriptor(
    name="HuggingFace API Key",
    description="The API Key for interacting with HuggingFace",
    required=True,
    sensitive=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)]
)
HUGGING_FACE_MODEL = PropertyDescriptor(
    name="HuggingFace Model",
    description="The name of the HuggingFace model to use",
    default_value="sentence-transformers/all-MiniLM-L6-v2",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    dependencies=[PropertyDependency(EMBEDDING_MODEL, HUGGING_FACE)]
)
OPENAI_API_KEY = PropertyDescriptor(
    name="OpenAI API Key",
    description="The API Key for OpenAI in order to create embeddings",
    required=True,
    sensitive=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)]
)
OPENAI_API_MODEL = PropertyDescriptor(
    name="OpenAI Model",
    description="The API Key for OpenAI in order to create embeddings",
    default_value="text-embedding-ada-002",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    dependencies=[PropertyDependency(EMBEDDING_MODEL, OPENAI)]
)
HTTP_HOST = PropertyDescriptor(
    name="HTTP Host",
    description="URL where OpenSearch is hosted.",
    default_value="http://localhost:9200",
    required=True,
    validators=[StandardValidators.URL_VALIDATOR]
)
USERNAME = PropertyDescriptor(
    name="Username",
    description="The username to use for authenticating to OpenSearch server",
    required=False,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR]
)
PASSWORD = PropertyDescriptor(
    name="Password",
    description="The password to use for authenticating to OpenSearch server",
    required=False,
    sensitive=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR]
)
INDEX_NAME = PropertyDescriptor(
    name="Index Name",
    description="The name of the OpenSearch index.",
    sensitive=False,
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)
VECTOR_FIELD = PropertyDescriptor(
    name="Vector Field Name",
    description="The name of field in the document where the embeddings are stored. This field need to be a 'knn_vector' typed field.",
    default_value="vector_field",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)
TEXT_FIELD = PropertyDescriptor(
    name="Text Field Name",
    description="The name of field in the document where the text is stored.",
    default_value="text",
    required=True,
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
)


def create_authentication_params(context):
    username = context.getProperty(USERNAME).getValue()
    password = context.getProperty(PASSWORD).getValue()

    params = {"verify_certs": "true"}

    if username is not None and password is not None:
        params["http_auth"] = (username, password)

    return params


def parse_documents(json_lines, id_field_name, file_name):
    import json

    texts = []
    metadatas = []
    ids = []
    for i, line in enumerate(json_lines.split("\n"), start=1):
        try:
            doc = json.loads(line)
        except Exception as e:
            raise ValueError(f"Could not parse line {i} as JSON") from e

        text = doc.get('text')
        metadata = doc.get('metadata')
        texts.append(text)

        # Remove any null values, or it will cause the embedding to fail
        filtered_metadata = {key: value for key, value in metadata.items() if value is not None}
        metadatas.append(filtered_metadata)

        doc_id = None
        if id_field_name is not None:
            doc_id = metadata.get(id_field_name)
        if doc_id is None:
            doc_id = file_name + "-" + str(i)
        ids.append(doc_id)

    return {"texts": texts, "metadatas": metadatas, "ids": ids}
