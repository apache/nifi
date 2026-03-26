/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createAction, props } from '@ngrx/store';
import { ConnectorDefinition } from './index';
import { DefinitionCoordinates } from '../index';

const CONNECTOR_DEFINITION_PREFIX = '[Connector Definition]';

export const loadConnectorDefinition = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Connector Definition`,
    props<{ coordinates: DefinitionCoordinates }>()
);

export const loadConnectorDefinitionSuccess = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Connector Definition Success`,
    props<{ connectorDefinition: ConnectorDefinition }>()
);

export const connectorDefinitionApiError = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Connector Definition Error`,
    props<{ error: string }>()
);

export const resetConnectorDefinitionState = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Reset Connector Definition State`
);

export const loadStepDocumentation = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Step Documentation`,
    props<{ coordinates: DefinitionCoordinates; stepName: string }>()
);

export const loadStepDocumentationSuccess = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Step Documentation Success`,
    props<{ stepName: string; documentation: string }>()
);

export const stepDocumentationApiError = createAction(
    `${CONNECTOR_DEFINITION_PREFIX} Load Step Documentation Error`,
    props<{ stepName: string; error: string }>()
);
