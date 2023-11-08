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
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.embeddings.huggingface import HuggingFaceInferenceAPIEmbeddings


# Embedding Functions
ONNX_ALL_MINI_LM_L6_V2 = "ONNX all-MiniLM-L6-v2 Model"
HUGGING_FACE = "Hugging Face Model"
OPENAI = "OpenAI Model"
SENTENCE_TRANSFORMERS = "Sentence Transformers"


EMBEDDING_FUNCTION = PropertyDescriptor(
    name="Embedding Function",
    description="Specifies which embedding function should be used in order to create embeddings from incoming Documents",
    allowable_values=[ONNX_ALL_MINI_LM_L6_V2, HUGGING_FACE, OPENAI, SENTENCE_TRANSFORMERS],
    default_value=ONNX_ALL_MINI_LM_L6_V2,
    required=True
)
HUGGING_FACE_MODEL_NAME = PropertyDescriptor(
    name="HuggingFace Model Name",
    description="The name of the HuggingFace model to use",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    default_value="sentence-transformers/all-MiniLM-L6-v2",
    required=True,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, HUGGING_FACE)]
)
HUGGING_FACE_API_KEY = PropertyDescriptor(
    name="HuggingFace API Key",
    description="The API Key for interacting with HuggingFace",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    sensitive=True,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, HUGGING_FACE)]
)
OPENAI_API_KEY = PropertyDescriptor(
    name="OpenAI API Key",
    description="The API Key for interacting with OpenAI",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=True,
    sensitive=True,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
OPENAI_MODEL_NAME = PropertyDescriptor(
    name="OpenAI Model Name",
    description="The name of the OpenAI model to use",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    default_value="text-embedding-ada-002",
    required=True,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
OPENAI_ORGANIZATION = PropertyDescriptor(
    name="OpenAI Organization ID",
    description="The OpenAI Organization ID",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
OPENAI_API_BASE = PropertyDescriptor(
    name="OpenAI API Base Path",
    description="The API Base to use for interacting with OpenAI. This is used for interacting with different deployments, such as an Azure deployment.",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
OPENAI_API_TYPE = PropertyDescriptor(
    name="OpenAI API Deployment Type",
    description="The type of the OpenAI API Deployment. This is used for interacting with different deployments, such as an Azure deployment.",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
OPENAI_API_VERSION = PropertyDescriptor(
    name="OpenAI API Version",
    description="The OpenAI API Version. This is used for interacting with different deployments, such as an Azure deployment.",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, OPENAI)]
)
SENTENCE_TRANSFORMER_MODEL_NAME = PropertyDescriptor(
    name="Sentence Transformer Model Name",
    description="The name of the Sentence Transformer model to use",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    default_value="all-MiniLM-L6-v2",
    required=True,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, SENTENCE_TRANSFORMERS)]
)
SENTENCE_TRANSFORMER_DEVICE = PropertyDescriptor(
    name="Sentence Transformer Device Type",
    description="""The type of device to use for performing the embeddings using the Sentence Transformer, such as 'cpu', 'cuda', 'mps', 'cuda:0', etc. 
                   If not specified, a GPU will be used if possible, otherwise a CPU.""",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False,
    dependencies=[PropertyDependency(EMBEDDING_FUNCTION, SENTENCE_TRANSFORMERS)]
)
EMBEDDING_MODEL = PropertyDescriptor(
    name="Embedding Model",
    description="Specifies which embedding model should be used in order to create embeddings from incoming Documents. Default model is OpenAI.",
    allowable_values=[HUGGING_FACE, OPENAI],
    default_value=OPENAI,
    required=True
)
PROPERTIES = [
    EMBEDDING_FUNCTION,
    HUGGING_FACE_MODEL_NAME,
    HUGGING_FACE_API_KEY,
    OPENAI_MODEL_NAME,
    OPENAI_API_KEY,
    OPENAI_ORGANIZATION,
    OPENAI_API_BASE,
    OPENAI_API_TYPE,
    OPENAI_API_VERSION,
    SENTENCE_TRANSFORMER_MODEL_NAME,
    SENTENCE_TRANSFORMER_DEVICE,
    EMBEDDING_MODEL
]


def create_embedding_function(context):
    from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2, OpenAIEmbeddingFunction, HuggingFaceEmbeddingFunction, SentenceTransformerEmbeddingFunction

    function_name = context.getProperty(EMBEDDING_FUNCTION).getValue()
    if function_name == ONNX_ALL_MINI_LM_L6_V2:
        return ONNXMiniLM_L6_V2()

    if function_name == OPENAI:
        api_key = context.getProperty(OPENAI_API_KEY).getValue()
        model_name = context.getProperty(OPENAI_MODEL_NAME).getValue()
        organization_id = context.getProperty(OPENAI_ORGANIZATION).getValue()
        api_base = context.getProperty(OPENAI_API_BASE).getValue()
        api_type = context.getProperty(OPENAI_API_TYPE).getValue()
        api_version = context.getProperty(OPENAI_API_VERSION).getValue()
        return OpenAIEmbeddingFunction(api_key=api_key, model_name=model_name, organization_id=organization_id, api_base=api_base, api_type=api_type, api_version=api_version)

    if function_name == HUGGING_FACE:
        api_key = context.getProperty(HUGGING_FACE_API_KEY).getValue()
        model_name = context.getProperty(HUGGING_FACE_MODEL_NAME).getValue()
        return HuggingFaceEmbeddingFunction(api_key=api_key, model_name=model_name)

    model_name = context.getProperty(SENTENCE_TRANSFORMER_MODEL_NAME).getValue()
    device = context.getProperty(SENTENCE_TRANSFORMER_DEVICE).getValue()
    return SentenceTransformerEmbeddingFunction(model_name=model_name, device=device)


def create_embedding_service(context):
    embedding_service = context.getProperty(EMBEDDING_MODEL).getValue()

    if embedding_service == OPENAI:
        openai_api_key = context.getProperty(OPENAI_API_KEY).getValue()
        return OpenAIEmbeddings(openai_api_key=openai_api_key)
    else:
        huggingface_api_key = context.getProperty(HUGGING_FACE_API_KEY).getValue()
        return HuggingFaceInferenceAPIEmbeddings(api_key=huggingface_api_key)
