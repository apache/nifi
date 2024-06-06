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
import { LoadCounterListingResponse, ResetCounterRequest, ResetCounterSuccess } from './index';
import { HttpErrorResponse } from '@angular/common/http';

const COUNTER_PREFIX = '[Counter Listing]';

export const loadCounters = createAction(`${COUNTER_PREFIX} Load Counter Listing`);

export const loadCountersSuccess = createAction(
    `${COUNTER_PREFIX} Load Counter Listing Success`,
    props<{ response: LoadCounterListingResponse }>()
);

export const counterListingApiError = createAction(
    `${COUNTER_PREFIX} Load Counter Listing Error`,
    props<{ errorResponse: HttpErrorResponse }>()
);

export const promptCounterReset = createAction(
    `${COUNTER_PREFIX} Prompt Counter Reset`,
    props<{ request: ResetCounterRequest }>()
);

export const resetCounter = createAction(`${COUNTER_PREFIX} Reset Counter`, props<{ request: ResetCounterRequest }>());

export const resetCounterSuccess = createAction(
    `${COUNTER_PREFIX} Reset Counter Success`,
    props<{ response: ResetCounterSuccess }>()
);

export const resetCounterState = createAction(`${COUNTER_PREFIX} Reset Counter State`);
