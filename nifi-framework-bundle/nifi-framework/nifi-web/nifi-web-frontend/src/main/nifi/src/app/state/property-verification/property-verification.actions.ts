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
import { ConfigurationAnalysisResponse, PropertyVerificationResponse, VerifyPropertiesRequestContext } from './index';

const VERIFICATION_STATE_PREFIX = '[Property Verification State]';

export const verifyProperties = createAction(
    `${VERIFICATION_STATE_PREFIX} Verify Properties`,
    props<{ request: VerifyPropertiesRequestContext }>()
);

export const getConfigurationAnalysisSuccess = createAction(
    `${VERIFICATION_STATE_PREFIX} Get Configuration Analysis`,
    props<{ response: ConfigurationAnalysisResponse }>()
);

export const startPollingPropertyVerification = createAction(
    `${VERIFICATION_STATE_PREFIX} Start Polling Property Verification`
);

export const pollPropertyVerification = createAction(`${VERIFICATION_STATE_PREFIX} Poll Property Verification`);

export const pollPropertyVerificationSuccess = createAction(
    `${VERIFICATION_STATE_PREFIX} Poll Property Verification Success`,
    props<{ response: PropertyVerificationResponse }>()
);

export const stopPollingPropertyVerification = createAction(
    `${VERIFICATION_STATE_PREFIX} Stop Polling Property Verification`
);

export const propertyVerificationComplete = createAction(`${VERIFICATION_STATE_PREFIX} Property Verification Complete`);

export const verifyPropertiesSuccess = createAction(
    `${VERIFICATION_STATE_PREFIX} Verify Properties Success`,
    props<{ response: PropertyVerificationResponse }>()
);

export const verifyPropertiesComplete = createAction(
    `${VERIFICATION_STATE_PREFIX} Verify Properties Complete`,
    props<{ response: PropertyVerificationResponse }>()
);

export const openPropertyVerificationProgressDialog = createAction(
    `${VERIFICATION_STATE_PREFIX} Open Property Verification Progress`
);

export const resetPropertyVerificationState = createAction(`${VERIFICATION_STATE_PREFIX} Reset`);

export const initiatePropertyVerification = createAction(
    `${VERIFICATION_STATE_PREFIX} Initiate Property Verification`,
    props<{ response: ConfigurationAnalysisResponse }>()
);
