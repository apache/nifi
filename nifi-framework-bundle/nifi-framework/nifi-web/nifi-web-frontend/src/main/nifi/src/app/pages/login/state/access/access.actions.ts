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
import { AccessApiError, LoadAccessResponse, LoginRequest } from './index';

export const loadAccess = createAction('[Access] Load Access');

export const loadAccessSuccess = createAction(
    '[Access] Load Access Success',
    props<{ response: LoadAccessResponse }>()
);

export const accessApiError = createAction('[Access] Load Access Error', props<{ error: AccessApiError }>());

export const login = createAction('[Access] Login', props<{ request: LoginRequest }>());

export const loginFailure = createAction('[Access] Login Failure', props<{ failure: string }>());

export const verifyAccess = createAction('[Access] Verify Access');

export const verifyAccessSuccess = createAction('[Access] Verify Access Success');
