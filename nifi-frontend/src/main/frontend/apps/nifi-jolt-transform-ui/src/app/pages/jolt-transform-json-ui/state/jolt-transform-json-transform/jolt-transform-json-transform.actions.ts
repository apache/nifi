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
import { TransformJoltSpecRequest, TransformJoltSpecSuccess } from './index';
import { HttpErrorResponse } from '@angular/common/http';

const JOLT_TRANSFORM_JSON_TRANSFORM_PREFIX = '[Jolt Transform Json Transform]';

export const resetJoltTransformJsonTransformState = createAction(
    `${JOLT_TRANSFORM_JSON_TRANSFORM_PREFIX} Reset Jolt Transform Json Transform State`
);

export const transformJoltSpec = createAction(
    `${JOLT_TRANSFORM_JSON_TRANSFORM_PREFIX} Transform Jolt Specification`,
    props<{ request: TransformJoltSpecRequest }>()
);

export const transformJoltSpecSuccess = createAction(
    `${JOLT_TRANSFORM_JSON_TRANSFORM_PREFIX} Transform Jolt Specification Success`,
    props<{ response: TransformJoltSpecSuccess }>()
);

export const transformJoltSpecFailure = createAction(
    `${JOLT_TRANSFORM_JSON_TRANSFORM_PREFIX} Transform Jolt Specification Failure`,
    props<{ response: HttpErrorResponse }>()
);
