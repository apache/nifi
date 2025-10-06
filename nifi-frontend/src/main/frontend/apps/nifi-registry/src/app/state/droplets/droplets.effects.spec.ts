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
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { Observable, of, throwError } from 'rxjs';
import { DropletsEffects } from './droplets.effects';
import { DropletsService } from '../../service/droplets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import * as DropletsActions from './droplets.actions';
import * as ErrorActions from '../../state/error/error.actions';
import { HttpErrorResponse, HttpHeaders, HttpResponse } from '@angular/common/http';
import { ErrorContextKey } from '../error';
import { ImportNewDropletDialogComponent } from '../../pages/resources/feature/ui/import-new-droplet-dialog/import-new-droplet-dialog.component';
import { DropletVersionsDialogComponent } from '../../pages/resources/feature/ui/droplet-versions-dialog/droplet-versions-dialog.component';
import { YesNoDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { Bucket } from '../buckets';

const createDroplet = (overrides = {}) => ({
    identifier: 'droplet-1',
    name: 'Test Droplet',
    description: 'Test Description',
    bucketIdentifier: 'bucket-1',
    bucketName: 'Test Bucket',
    createdTimestamp: 1632924000000,
    modifiedTimestamp: 1632924000000,
    type: 'FLOW',
    permissions: {
        canRead: true,
        canWrite: true,
        canDelete: true
    },
    revision: {
        version: 1
    },
    link: {
        href: '/nifi-registry-api/buckets/bucket-1/flows/droplet-1',
        params: {
            rel: 'self'
        }
    },
    versionCount: 1,
    ...overrides
});

const createBucket = (overrides = {}): Bucket => ({
    identifier: 'bucket-1',
    name: 'Test Bucket',
    description: 'Test Description',
    createdTimestamp: 1632924000000,
    allowBundleRedeploy: false,
    allowPublicRead: false,
    permissions: {
        canRead: true,
        canWrite: true
    },
    revision: {
        version: 1
    },
    link: {
        href: '/nifi-registry-api/buckets/bucket-1',
        params: {
            rel: 'self'
        }
    },
    ...overrides
});

describe('DropletsEffects', () => {
    let actions$: Observable<Action>;
    let effects: DropletsEffects;
    let dropletsService: jest.Mocked<DropletsService>;
    let errorHelper: jest.Mocked<ErrorHelper>;
    let dialog: jest.Mocked<MatDialog>;
    let store: Store;

    beforeEach(() => {
        const mockDropletsService = {
            getDroplets: jest.fn(),
            deleteDroplet: jest.fn(),
            createNewDroplet: jest.fn(),
            uploadDroplet: jest.fn(),
            exportDropletVersionedSnapshot: jest.fn(),
            getDropletSnapshotMetadata: jest.fn()
        };

        const mockErrorHelper = {
            getErrorString: jest.fn()
        };

        const mockDialog = {
            open: jest.fn(),
            closeAll: jest.fn()
        };

        TestBed.configureTestingModule({
            providers: [
                DropletsEffects,
                provideMockActions(() => actions$),
                provideMockStore(),
                { provide: DropletsService, useValue: mockDropletsService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatDialog, useValue: mockDialog }
            ]
        });

        effects = TestBed.inject(DropletsEffects);
        dropletsService = TestBed.inject(DropletsService) as jest.Mocked<DropletsService>;
        errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        dialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        store = TestBed.inject(Store);
        jest.spyOn(store, 'dispatch');
    });

    describe('loadDroplets$', () => {
        it('should return loadDropletsSuccess with droplets on success', (done) => {
            const droplets = [createDroplet(), createDroplet({ identifier: 'droplet-2' })];
            dropletsService.getDroplets.mockReturnValue(of(droplets));

            actions$ = of(DropletsActions.loadDroplets());

            effects.loadDroplets$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.loadDropletsSuccess({
                        response: { droplets }
                    })
                );
                done();
            });
        });

        it('should return error action on failure', (done) => {
            const error = new HttpErrorResponse({ status: 404, statusText: 'Not Found' });
            dropletsService.getDroplets.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error loading droplets');

            actions$ = of(DropletsActions.loadDroplets());

            effects.loadDroplets$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.dropletsBannerError({
                        errorContext: {
                            errors: ['Error loading droplets'],
                            context: ErrorContextKey.GLOBAL
                        }
                    })
                );
                done();
            });
        });
    });

    describe('openDeleteDropletDialog$', () => {
        it('should open delete confirmation dialog and dispatch delete action on confirmation', (done) => {
            const droplet = createDroplet();
            const mockDialogRef = {
                componentInstance: {
                    yes: of(true)
                }
            };
            dialog.open.mockReturnValue(mockDialogRef as any);

            actions$ = of(DropletsActions.openDeleteDropletDialog({ request: { droplet } }));

            effects.openDeleteDropletDialog$.subscribe(() => {
                expect(dialog.open).toHaveBeenCalledWith(
                    YesNoDialog,
                    expect.objectContaining({
                        data: {
                            title: 'Delete resource',
                            message: `This action will delete all versions of ${droplet.name}`
                        }
                    })
                );
                expect(store.dispatch).toHaveBeenCalledWith(
                    DropletsActions.deleteDroplet({
                        request: { droplet }
                    })
                );
                done();
            });
        });
    });

    describe('deleteDroplet$', () => {
        const droplet = createDroplet();

        it('should return deleteDropletSuccess on success', (done) => {
            dropletsService.deleteDroplet.mockReturnValue(of(droplet));

            actions$ = of(DropletsActions.deleteDroplet({ request: { droplet } }));

            effects.deleteDroplet$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.deleteDropletSuccess({
                        response: droplet
                    })
                );
                done();
            });
        });

        it('should return error actions on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            dropletsService.deleteDroplet.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error deleting droplet');

            actions$ = of(DropletsActions.deleteDroplet({ request: { droplet } }));

            let actionCount = 0;
            effects.deleteDroplet$.subscribe((action) => {
                if (actionCount === 0) {
                    expect(action).toEqual(DropletsActions.deleteDropletFailure());
                } else {
                    expect(action).toEqual(
                        ErrorActions.snackBarError({
                            error: 'Error deleting droplet'
                        })
                    );
                    done();
                }
                actionCount++;
            });
        });
    });

    describe('openImportNewDropletDialog$', () => {
        it('should open import new droplet dialog', (done) => {
            const buckets = [createBucket(), createBucket({ identifier: 'bucket-2' })];

            actions$ = of(DropletsActions.openImportNewDropletDialog({ request: { buckets } }));

            effects.openImportNewDropletDialog$.subscribe(() => {
                expect(dialog.open).toHaveBeenCalledWith(
                    ImportNewDropletDialogComponent,
                    expect.objectContaining({
                        autoFocus: false,
                        data: { buckets }
                    })
                );
                done();
            });
        });
    });

    describe('createNewDroplet$', () => {
        const bucket = createBucket();
        const request = {
            bucket,
            name: 'New Droplet',
            description: 'New Description',
            file: new File([], 'test.json')
        };

        it('should return createNewDropletSuccess and trigger version import on success', (done) => {
            const newDroplet = createDroplet({ name: request.name, description: request.description });
            dropletsService.createNewDroplet.mockReturnValue(of(newDroplet));

            actions$ = of(DropletsActions.createNewDroplet({ request }));

            effects.createNewDroplet$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.createNewDropletSuccess({
                        response: newDroplet,
                        request: {
                            href: newDroplet.link.href,
                            file: request.file,
                            description: request.description
                        }
                    })
                );
                done();
            });
        });

        it('should return error action on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            dropletsService.createNewDroplet.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error creating droplet');

            actions$ = of(DropletsActions.createNewDroplet({ request }));

            effects.createNewDroplet$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.dropletsBannerError({
                        errorContext: {
                            errors: ['Error creating droplet'],
                            context: ErrorContextKey.CREATE_DROPLET
                        }
                    })
                );
                done();
            });
        });
    });

    describe('importNewDropletVersion$', () => {
        const droplet = createDroplet();
        const request = {
            href: droplet.link.href,
            file: new File([], 'test.json'),
            description: 'New Version Description'
        };

        it('should return importNewDropletVersionSuccess on success', (done) => {
            dropletsService.uploadDroplet.mockReturnValue(of(droplet));

            actions$ = of(DropletsActions.importNewDropletVersion({ request }));

            effects.importNewDroplet$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.importNewDropletVersionSuccess({
                        response: droplet
                    })
                );
                done();
            });
        });

        it('should return error action on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            dropletsService.uploadDroplet.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error importing version');

            actions$ = of(DropletsActions.importNewDropletVersion({ request }));

            effects.importNewDroplet$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.dropletsBannerError({
                        errorContext: {
                            errors: ['Error importing version'],
                            context: ErrorContextKey.IMPORT_DROPLET_VERSION
                        }
                    })
                );
                done();
            });
        });
    });

    describe('exportDropletVersion$', () => {
        const droplet = createDroplet();
        const request = {
            droplet,
            version: 1
        };

        it('should return exportDropletVersionSuccess and trigger download on success', (done) => {
            const headers = new HttpHeaders().set('Filename', 'test.json');
            const mockResponse = new HttpResponse({
                body: JSON.stringify({ content: 'test' }),
                headers
            });
            dropletsService.exportDropletVersionedSnapshot.mockReturnValue(of(mockResponse));

            // Mock document methods
            const mockAnchor = {
                href: '',
                download: '',
                setAttribute: jest.fn(),
                click: jest.fn()
            };
            jest.spyOn(document, 'createElement').mockReturnValue(mockAnchor as any);
            jest.spyOn(document.body, 'appendChild').mockImplementation();
            jest.spyOn(document.body, 'removeChild').mockImplementation();

            actions$ = of(DropletsActions.exportDropletVersion({ request }));

            effects.exportDropletVersion$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.exportDropletVersionSuccess({
                        response: mockResponse
                    })
                );
                expect(document.createElement).toHaveBeenCalledWith('a');
                expect(mockAnchor.click).toHaveBeenCalled();
                done();
            });
        });

        it('should return error action on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            dropletsService.exportDropletVersionedSnapshot.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error exporting version');

            actions$ = of(DropletsActions.exportDropletVersion({ request }));

            effects.exportDropletVersion$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.dropletsBannerError({
                        errorContext: {
                            errors: ['Error exporting version'],
                            context: ErrorContextKey.EXPORT_DROPLET_VERSION
                        }
                    })
                );
                done();
            });
        });
    });

    describe('openDropletVersionsDialog$', () => {
        it('should open versions dialog with metadata', (done) => {
            const droplet = createDroplet();
            const versions = [{ version: 1, userIdentity: 'user1', timestamp: Date.now() }];
            dropletsService.getDropletSnapshotMetadata.mockReturnValue(of(versions));

            actions$ = of(DropletsActions.openDropletVersionsDialog({ request: { droplet } }));

            effects.openDropletVersionsDialog$.subscribe((action) => {
                expect(dialog.open).toHaveBeenCalledWith(
                    DropletVersionsDialogComponent,
                    expect.objectContaining({
                        autoFocus: false,
                        data: { droplet, versions }
                    })
                );
                expect(action).toEqual(DropletsActions.noOp());
                done();
            });
        });

        it('should return error action on metadata fetch failure', (done) => {
            const droplet = createDroplet();
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            dropletsService.getDropletSnapshotMetadata.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error fetching versions');

            actions$ = of(DropletsActions.openDropletVersionsDialog({ request: { droplet } }));

            effects.openDropletVersionsDialog$.subscribe((action) => {
                expect(action).toEqual(
                    DropletsActions.dropletsBannerError({
                        errorContext: {
                            errors: ['Error fetching versions'],
                            context: ErrorContextKey.GLOBAL
                        }
                    })
                );
                done();
            });
        });
    });

    describe('dialog closing effects', () => {
        it('should close dialogs on deleteDropletSuccess', (done) => {
            actions$ = of(DropletsActions.deleteDropletSuccess({ response: createDroplet() }));

            effects.deleteDropletSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });

        it('should close dialogs on importNewDropletVersionSuccess', (done) => {
            actions$ = of(DropletsActions.importNewDropletVersionSuccess({ response: createDroplet() }));

            effects.importNewDropletSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });

        it('should close dialogs on exportDropletVersionSuccess', (done) => {
            const headers = new HttpHeaders().set('Filename', 'test.json');
            const mockResponse = new HttpResponse({
                body: JSON.stringify({ content: 'test' }),
                headers
            });
            actions$ = of(DropletsActions.exportDropletVersionSuccess({ response: mockResponse }));

            effects.exportDropletVersionSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });
    });
});
