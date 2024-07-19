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
import { SavePropertiesRequest, SavePropertiesSuccess } from './index';
import { HttpErrorResponse } from '@angular/common/http';

const JOLT_TRANSFORM_JSON_PROPERTY_PREFIX = '[Jolt Transform Json Properties]';

export const resetJoltTransformJsonPropertyState = createAction(
    `${JOLT_TRANSFORM_JSON_PROPERTY_PREFIX} Reset Jolt Transform Json Property State`
);

export const saveProperties = createAction(
    `${JOLT_TRANSFORM_JSON_PROPERTY_PREFIX} Save Properties`,
    props<{ request: SavePropertiesRequest }>()
);

export const savePropertiesSuccess = createAction(
    `${JOLT_TRANSFORM_JSON_PROPERTY_PREFIX} Save Properties Success`,
    props<{ response: SavePropertiesSuccess }>()
);

export const savePropertiesFailure = createAction(
    `${JOLT_TRANSFORM_JSON_PROPERTY_PREFIX} Save Properties Failure`,
    props<{ response: HttpErrorResponse }>()
);

export const resetSavePropertiesState = createAction(
    `${JOLT_TRANSFORM_JSON_PROPERTY_PREFIX} Reset Validate Transform and Spec Ui State`
);
