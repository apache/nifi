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

import { initialState, parameterContextListingReducer } from './parameter-context-listing.reducer';
import {
    deleteParameterContextUpdateRequestSuccess,
    editParameterContextComplete,
    parameterContextListingBannerApiError,
    submitParameterContextUpdateRequest
} from './parameter-context-listing.actions';
import { ParameterContextListingState } from './index';

describe('ParameterContextListing Reducer', () => {
    describe('submitParameterContextUpdateRequest', () => {
        it('should persist postUpdateNavigation fields from the request', () => {
            const result = parameterContextListingReducer(
                initialState,
                submitParameterContextUpdateRequest({
                    request: {
                        id: 'pc-1',
                        payload: {},
                        postUpdateNavigation: ['/parameter-contexts', 'inherited-id', 'edit'],
                        postUpdateNavigationBoundary: ['/parameter-contexts'],
                        postUpdateNavigationState: { highlightedParameterName: 'param-a' }
                    }
                })
            );

            expect(result.saving).toBe(true);
            expect(result.updateRequestParameterContextId).toBe('pc-1');
            expect(result.postUpdateNavigation).toEqual(['/parameter-contexts', 'inherited-id', 'edit']);
            expect(result.postUpdateNavigationBoundary).toEqual(['/parameter-contexts']);
            expect(result.postUpdateNavigationState).toEqual({ highlightedParameterName: 'param-a' });
        });

        it('should clear postUpdateNavigation fields when not provided on submit', () => {
            const stateWithPendingNav: ParameterContextListingState = {
                ...initialState,
                postUpdateNavigation: ['/parameter-contexts', 'old-id', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'old' }
            };

            const result = parameterContextListingReducer(
                stateWithPendingNav,
                submitParameterContextUpdateRequest({
                    request: {
                        id: 'pc-1',
                        payload: {}
                    }
                })
            );

            expect(result.postUpdateNavigation).toBeNull();
            expect(result.postUpdateNavigationBoundary).toBeNull();
            expect(result.postUpdateNavigationState).toBeNull();
        });
    });

    describe('deleteParameterContextUpdateRequestSuccess', () => {
        it('should keep postUpdateNavigation fields for the review CTA', () => {
            const stateWithPendingNav: ParameterContextListingState = {
                ...initialState,
                saving: true,
                postUpdateNavigation: ['/parameter-contexts', 'inherited-id', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'param-a' },
                updateRequestEntity: {
                    request: {
                        requestId: 'req-1',
                        uri: '',
                        lastUpdated: '',
                        complete: true,
                        percentComponent: 100,
                        state: 'Complete',
                        updateSteps: [],
                        referencingComponents: []
                    },
                    parameterContextRevision: { version: 1 }
                }
            };

            const result = parameterContextListingReducer(
                stateWithPendingNav,
                deleteParameterContextUpdateRequestSuccess({
                    response: {
                        requestEntity: stateWithPendingNav.updateRequestEntity!
                    }
                })
            );

            expect(result.saving).toBe(false);
            expect(result.postUpdateNavigation).toEqual(['/parameter-contexts', 'inherited-id', 'edit']);
            expect(result.postUpdateNavigationBoundary).toEqual(['/parameter-contexts']);
            expect(result.postUpdateNavigationState).toEqual({ highlightedParameterName: 'param-a' });
        });
    });

    describe('parameterContextListingBannerApiError', () => {
        it('should clear postUpdateNavigation fields on error', () => {
            const stateWithPendingNav: ParameterContextListingState = {
                ...initialState,
                saving: true,
                postUpdateNavigation: ['/parameter-contexts', 'inherited-id', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'param-a' }
            };

            const result = parameterContextListingReducer(
                stateWithPendingNav,
                parameterContextListingBannerApiError({ error: 'boom' })
            );

            expect(result.saving).toBe(false);
            expect(result.postUpdateNavigation).toBeNull();
            expect(result.postUpdateNavigationBoundary).toBeNull();
            expect(result.postUpdateNavigationState).toBeNull();
        });
    });

    describe('editParameterContextComplete', () => {
        it('should clear postUpdateNavigation fields when the dialog closes', () => {
            const stateWithPendingNav: ParameterContextListingState = {
                ...initialState,
                postUpdateNavigation: ['/parameter-contexts', 'inherited-id', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'param-a' }
            };

            const result = parameterContextListingReducer(stateWithPendingNav, editParameterContextComplete());

            expect(result.postUpdateNavigation).toBeNull();
            expect(result.postUpdateNavigationBoundary).toBeNull();
            expect(result.postUpdateNavigationState).toBeNull();
        });
    });
});
