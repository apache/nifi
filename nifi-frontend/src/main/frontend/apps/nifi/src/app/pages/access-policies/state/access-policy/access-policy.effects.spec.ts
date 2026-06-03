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
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { ReplaySubject, of, take, throwError } from 'rxjs';

import { AccessPolicyEffects } from './access-policy.effects';
import * as AccessPolicyActions from './access-policy.actions';
import { AccessPolicyService } from '../../service/access-policy.service';
import { AccessPolicyEntity, Action as PolicyAction, PolicyStatus, ResourceAction } from '../shared';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';

describe('AccessPolicyEffects', () => {
    let action$: ReplaySubject<Action>;

    function createAccessPolicyEntity(resource: string): AccessPolicyEntity {
        return {
            id: 'policy-1',
            component: {
                id: 'policy-1',
                resource,
                action: PolicyAction.Read,
                users: [],
                userGroups: [],
                configurable: true
            },
            revision: { version: 0, clientId: 'client-id' },
            uri: '/policies/policy-1',
            permissions: { canRead: true, canWrite: true },
            generated: '2026-01-01 00:00:00 UTC'
        } as AccessPolicyEntity;
    }

    async function setup() {
        await TestBed.configureTestingModule({
            providers: [
                AccessPolicyEffects,
                AccessPolicyService,
                provideHttpClient(),
                provideHttpClientTesting(),
                provideMockActions(() => action$),
                provideMockStore(),
                {
                    provide: Client,
                    useValue: {
                        getClientId: () => 'client-id',
                        getRevision: (entity: { revision: unknown }) => entity.revision
                    }
                },
                {
                    provide: ClusterConnectionService,
                    useValue: { isDisconnectionAcknowledged: () => false }
                },
                { provide: ErrorHelper, useValue: { getErrorString: vi.fn().mockReturnValue('error') } },
                { provide: MatDialog, useValue: { open: vi.fn() } },
                { provide: Router, useValue: { navigate: vi.fn() } }
            ]
        }).compileComponents();

        const effects = TestBed.inject(AccessPolicyEffects);
        const accessPolicyService = TestBed.inject(AccessPolicyService);

        return { effects, accessPolicyService };
    }

    async function dispatchLoadAccessPolicy(
        effects: AccessPolicyEffects,
        resourceAction: ResourceAction
    ): Promise<Action> {
        action$.next(
            AccessPolicyActions.loadAccessPolicy({
                request: { resourceAction }
            })
        );

        return new Promise((resolve) => {
            effects.loadAccessPolicy$.pipe(take(1)).subscribe(resolve);
        });
    }

    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadAccessPolicy$', () => {
        it('should mark connector data policy as Found when API resource matches buildResourcePath', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                resourceIdentifier: 'data',
                action: PolicyAction.Read
            };
            const accessPolicy = createAccessPolicyEntity('/data/connectors');

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(of(accessPolicy) as never);

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.loadAccessPolicySuccess({
                    response: {
                        accessPolicy,
                        policyStatus: PolicyStatus.Found
                    }
                })
            );
            expect(accessPolicyService.getAccessPolicy).toHaveBeenCalledWith(resourceAction);
        });

        it('should mark connector data policy as Inherited when API resource differs from buildResourcePath', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                resourceIdentifier: 'data',
                action: PolicyAction.Read
            };
            const accessPolicy = createAccessPolicyEntity('/connectors');

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(of(accessPolicy) as never);

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.loadAccessPolicySuccess({
                    response: {
                        accessPolicy,
                        policyStatus: PolicyStatus.Inherited
                    }
                })
            );
        });

        it('should mark connector provenance policy as Found when API resource matches buildResourcePath', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                resourceIdentifier: 'provenance-data',
                action: PolicyAction.Read
            };
            const accessPolicy = createAccessPolicyEntity('/provenance-data/connectors');

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(of(accessPolicy) as never);

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.loadAccessPolicySuccess({
                    response: {
                        accessPolicy,
                        policyStatus: PolicyStatus.Found
                    }
                })
            );
        });

        it('should mark global connectors policy as Found when API resource is /connectors', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                action: PolicyAction.Write
            };
            const accessPolicy = createAccessPolicyEntity('/connectors');

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(of(accessPolicy) as never);

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.loadAccessPolicySuccess({
                    response: {
                        accessPolicy,
                        policyStatus: PolicyStatus.Found
                    }
                })
            );
        });

        it('should treat untransformed connector data path as Inherited', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                resourceIdentifier: 'data',
                action: PolicyAction.Read
            };
            const accessPolicy = createAccessPolicyEntity('/connectors/data');

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(of(accessPolicy) as never);

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.loadAccessPolicySuccess({
                    response: {
                        accessPolicy,
                        policyStatus: PolicyStatus.Inherited
                    }
                })
            );
        });

        it('should reset access policy with NotFound on 404', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                action: PolicyAction.Read
            };

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(
                throwError(() => new HttpErrorResponse({ status: 404 })) as never
            );

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.resetAccessPolicy({
                    response: {
                        policyStatus: PolicyStatus.NotFound
                    }
                })
            );
        });

        it('should reset access policy with Forbidden on 403', async () => {
            const { effects, accessPolicyService } = await setup();
            const resourceAction: ResourceAction = {
                resource: 'connectors',
                resourceIdentifier: 'data',
                action: PolicyAction.Read
            };

            vi.spyOn(accessPolicyService, 'getAccessPolicy').mockReturnValue(
                throwError(() => new HttpErrorResponse({ status: 403 })) as never
            );

            const result = await dispatchLoadAccessPolicy(effects, resourceAction);

            expect(result).toEqual(
                AccessPolicyActions.resetAccessPolicy({
                    response: {
                        policyStatus: PolicyStatus.Forbidden
                    }
                })
            );
        });
    });
});
