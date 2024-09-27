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
import { ControllerServiceDefinition } from './index';
import { DefinitionCoordinates } from '../index';

const CONTROLLER_SERVICE_DEFINITION_PREFIX = '[Controller Service Definition]';

export const loadControllerServiceDefinition = createAction(
    `${CONTROLLER_SERVICE_DEFINITION_PREFIX} Load Controller Service Definition`,
    props<{ coordinates: DefinitionCoordinates }>()
);

export const loadControllerServiceDefinitionSuccess = createAction(
    `${CONTROLLER_SERVICE_DEFINITION_PREFIX} Load Controller Service Definition Success`,
    props<{ controllerServiceDefinition: ControllerServiceDefinition }>()
);

export const controllerServiceDefinitionApiError = createAction(
    `${CONTROLLER_SERVICE_DEFINITION_PREFIX} Load Controller Service Definition Error`,
    props<{ error: string }>()
);

export const resetControllerServiceDefinitionState = createAction(
    `${CONTROLLER_SERVICE_DEFINITION_PREFIX} Reset Controller Service Definition State`
);
