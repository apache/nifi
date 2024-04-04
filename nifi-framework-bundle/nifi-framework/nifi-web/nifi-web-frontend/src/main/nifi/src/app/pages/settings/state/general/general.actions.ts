/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createAction, props } from '@ngrx/store';
import { ControllerConfigResponse, UpdateControllerConfigRequest } from './index';

export const resetGeneralState = createAction('[General] Reset General State');

export const loadControllerConfig = createAction('[General] Load Controller Config');

export const loadControllerConfigSuccess = createAction(
    '[General] Load Controller Config Success',
    props<{ response: ControllerConfigResponse }>()
);

export const controllerConfigApiError = createAction(
    '[General] Load Controller Config Error',
    props<{ error: string }>()
);

export const updateControllerConfig = createAction(
    '[General] Set Max Timer Driven Thread Count',
    props<{ request: UpdateControllerConfigRequest }>()
);

export const updateControllerConfigSuccess = createAction(
    '[General] Update Controller Config Success',
    props<{ response: ControllerConfigResponse }>()
);
