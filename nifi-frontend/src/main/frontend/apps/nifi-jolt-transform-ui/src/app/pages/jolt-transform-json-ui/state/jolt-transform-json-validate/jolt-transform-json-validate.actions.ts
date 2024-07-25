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
import { ValidateJoltSpecRequest, ValidateJoltSpecSuccess } from './index';
import { HttpErrorResponse } from '@angular/common/http';

const JOLT_TRANSFORM_JSON_VALIDATE_PREFIX = '[Jolt Transform Json Validate]';

export const resetJoltTransformJsonValidateState = createAction(
    `${JOLT_TRANSFORM_JSON_VALIDATE_PREFIX} Reset Jolt Transform Json Validate State`
);

export const validateJoltSpec = createAction(
    `${JOLT_TRANSFORM_JSON_VALIDATE_PREFIX} Validate Jolt Specification`,
    props<{ request: ValidateJoltSpecRequest }>()
);

export const validateJoltSpecSuccess = createAction(
    `${JOLT_TRANSFORM_JSON_VALIDATE_PREFIX} Validate Jolt Specification Success`,
    props<{ response: ValidateJoltSpecSuccess }>()
);

export const validateJoltSpecFailure = createAction(
    `${JOLT_TRANSFORM_JSON_VALIDATE_PREFIX} Validate Jolt Specification Failure`,
    props<{ response: HttpErrorResponse }>()
);

export const resetValidateJoltSpecState = createAction(
    `${JOLT_TRANSFORM_JSON_VALIDATE_PREFIX} Reset Validate Transform and Spec Ui State`
);
