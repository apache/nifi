/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { createAction, props } from '@ngrx/store';
import { SelectProcessorStatusRequest, SummaryListingResponse } from './index';

const SUMMARY_LISTING_PREFIX: string = '[Summary Listing]';

export const loadSummaryListing = createAction(
    `${SUMMARY_LISTING_PREFIX} Load Summary Listing`,
    props<{ recursive: boolean }>()
);

export const loadSummaryListingSuccess = createAction(
    `${SUMMARY_LISTING_PREFIX} Load Summary Listing Success`,
    props<{ response: SummaryListingResponse }>()
);

export const summaryListingApiError = createAction(
    `${SUMMARY_LISTING_PREFIX} Load Summary Listing error`,
    props<{ error: string }>()
);

export const selectProcessorStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Processor Status`,
    props<{ request: SelectProcessorStatusRequest }>()
);

export const navigateToViewProcessorStatusHistory = createAction(
    `${SUMMARY_LISTING_PREFIX} Navigate To Processor Status History`,
    props<{ id: string }>()
);
