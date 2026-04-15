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

import { TestBed } from '@angular/core/testing';
import { CreateConnector } from './create-connector.component';
import { MatDialogRef } from '@angular/material/dialog';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { NEVER } from 'rxjs';
import { provideMockStore } from '@ngrx/store/testing';
import { DocumentedType } from '../../../../state/shared';
import { extensionTypesFeatureKey } from '../../../../state/extension-types';
import { initialExtensionsTypesState } from '../../../../state/extension-types/extension-types.reducer';
import { connectorsFeatureKey, connectorsListingFeatureKey } from '../../state';
import { initialState as initialConnectorsListingState } from '../../state/connectors-listing/connectors-listing.reducer';
import { purgeConnectorFeatureKey } from '../../state/purge-connector';
import { initialState as initialPurgeConnectorState } from '../../state/purge-connector/purge-connector.reducer';

describe('CreateConnector', () => {
    function createMockConnectorType(
        options: {
            type?: string;
            description?: string;
            restricted?: boolean;
        } = {}
    ): DocumentedType {
        return {
            type: options.type || 'org.apache.nifi.connector.TestConnector',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-nar',
                version: '1.0.0'
            },
            description: options.description || 'Test Connector',
            tags: ['test'],
            restricted: options.restricted ?? false
        };
    }

    async function setup() {
        const mockDialogRef = {
            keydownEvents: () => NEVER,
            close: vi.fn()
        };

        await TestBed.configureTestingModule({
            imports: [CreateConnector],
            providers: [
                provideMockStore({
                    initialState: {
                        [extensionTypesFeatureKey]: initialExtensionsTypesState,
                        [connectorsFeatureKey]: {
                            [connectorsListingFeatureKey]: initialConnectorsListingState,
                            [purgeConnectorFeatureKey]: initialPurgeConnectorState
                        }
                    }
                }),
                { provide: MatDialogRef, useValue: mockDialogRef }
            ],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        const fixture = TestBed.createComponent(CreateConnector);
        const component = fixture.componentInstance;

        fixture.detectChanges();

        return { component, fixture, mockDialogRef };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should emit createConnector event when connector type is selected', async () => {
        const connectorType = createMockConnectorType();
        const { component } = await setup();

        vi.spyOn(component.createConnector, 'emit');

        component.createNewConnector(connectorType);

        expect(component.createConnector.emit).toHaveBeenCalledWith(connectorType);
    });
});
