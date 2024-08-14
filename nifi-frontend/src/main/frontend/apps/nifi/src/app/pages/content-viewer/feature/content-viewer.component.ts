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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { loadContentViewerOptions, resetContentViewerOptions } from '../state/viewer-options/viewer-options.actions';
import { FormBuilder, FormGroup } from '@angular/forms';
import { selectBundledViewerOptions, selectViewerOptions } from '../state/viewer-options/viewer-options.selectors';
import { ContentViewer, HEX_VIEWER_URL, SupportedMimeTypes } from '../state/viewer-options';
import { isDefinedAndNotNull, SelectGroup, SelectOption, selectQueryParams } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { concatLatestFrom } from '@ngrx/operators';
import { navigateToBundledContentViewer, resetContent, setRef } from '../state/content/content.actions';
import { MatSelectChange } from '@angular/material/select';
import { loadAbout } from '../../../state/about/about.actions';
import { selectAbout } from '../../../state/about/about.selectors';
import { filter, map, switchMap, take } from 'rxjs';
import { navigateToExternalViewer } from '../state/external-viewer/external-viewer.actions';

@Component({
    selector: 'content-viewer',
    templateUrl: './content-viewer.component.html',
    styleUrls: ['./content-viewer.component.scss']
})
export class ContentViewerComponent implements OnInit, OnDestroy {
    viewerForm: FormGroup;
    viewAsOptions: SelectGroup[] = [];

    private supportedMimeTypeId = 0;
    private supportedMimeTypeLookup: Map<number, SupportedMimeTypes> = new Map<number, SupportedMimeTypes>();
    private supportedMimeTypeContentViewerLookup: Map<number, ContentViewer> = new Map<number, ContentViewer>();
    private mimeTypeDisplayNameLookup: Map<number, string> = new Map<number, string>();
    private mimeTypeIdsSupportedByBundledUis: Set<number> = new Set<number>();

    private defaultSupportedMimeTypeId: number | null = null;

    viewerSelected: boolean = false;
    private mimeType: string | undefined;
    private clientId: string | undefined;

    private queryParamsLoaded = false;
    private viewerOptionsLoaded = false;

    constructor(
        private store: Store<NiFiState>,
        private formBuilder: FormBuilder
    ) {
        this.viewerForm = this.formBuilder.group({ viewAs: null });

        this.store
            .select(selectViewerOptions)
            .pipe(
                isDefinedAndNotNull(),
                concatLatestFrom(() => this.store.select(selectBundledViewerOptions)),
                takeUntilDestroyed()
            )
            .subscribe(([externalViewerOptions, bundledViewerOptions]) => {
                this.supportedMimeTypeLookup.clear();
                this.supportedMimeTypeContentViewerLookup.clear();
                this.mimeTypeIdsSupportedByBundledUis.clear();
                this.mimeTypeDisplayNameLookup.clear();

                // maps a given content (by display name) to the supported mime type id
                // which can be used to look up the corresponding content viewer
                const supportedMimeTypeMapping = new Map<string, number[]>();

                // process all external viewer options
                externalViewerOptions.forEach((contentViewer) => {
                    contentViewer.supportedMimeTypes.forEach((supportedMimeType) => {
                        const supportedMimeTypeId = this.supportedMimeTypeId++;

                        if (!supportedMimeTypeMapping.has(supportedMimeType.displayName)) {
                            supportedMimeTypeMapping.set(supportedMimeType.displayName, []);
                        }
                        supportedMimeTypeMapping.get(supportedMimeType.displayName)?.push(supportedMimeTypeId);

                        this.supportedMimeTypeLookup.set(supportedMimeTypeId, supportedMimeType);
                        this.supportedMimeTypeContentViewerLookup.set(supportedMimeTypeId, contentViewer);
                    });
                });

                // process all bundled options
                bundledViewerOptions.forEach((contentViewer) => {
                    contentViewer.supportedMimeTypes.forEach((supportedMimeType) => {
                        const supportedMimeTypeId = this.supportedMimeTypeId++;

                        if (contentViewer.uri === HEX_VIEWER_URL) {
                            this.defaultSupportedMimeTypeId = supportedMimeTypeId;
                        }

                        if (!supportedMimeTypeMapping.has(supportedMimeType.displayName)) {
                            supportedMimeTypeMapping.set(supportedMimeType.displayName, []);
                        }
                        supportedMimeTypeMapping.get(supportedMimeType.displayName)?.push(supportedMimeTypeId);

                        this.mimeTypeIdsSupportedByBundledUis.add(supportedMimeTypeId);
                        this.supportedMimeTypeLookup.set(supportedMimeTypeId, supportedMimeType);
                        this.supportedMimeTypeContentViewerLookup.set(supportedMimeTypeId, contentViewer);
                    });
                });

                const newViewAsOptions: SelectGroup[] = [];
                supportedMimeTypeMapping.forEach((contentViewers, displayName) => {
                    const options: SelectOption[] = [];
                    contentViewers.forEach((contentViewerId) => {
                        this.mimeTypeDisplayNameLookup.set(contentViewerId, displayName);

                        const contentViewer = this.supportedMimeTypeContentViewerLookup.get(contentViewerId);
                        if (contentViewer) {
                            const option: SelectOption = {
                                text: contentViewer.displayName,
                                value: String(contentViewerId)
                            };
                            options.push(option);
                        }
                    });
                    const groupOption: SelectGroup = {
                        text: displayName,
                        options
                    };
                    newViewAsOptions.push(groupOption);
                });

                this.viewAsOptions = newViewAsOptions;
                this.viewerOptionsLoaded = true;

                if (this.queryParamsLoaded) {
                    this.handleDefaultSelection();
                }
            });

        this.store
            .select(selectQueryParams)
            .pipe(
                takeUntilDestroyed(),
                switchMap((queryParams) => {
                    return this.store.select(selectAbout).pipe(
                        isDefinedAndNotNull(),
                        take(1),
                        filter((about) => {
                            const dataRef: string | undefined = queryParams['ref'];
                            if (dataRef) {
                                // this check is used to ensure the data ref which is supplied through a query
                                // param will attempt to load content from this specific NiFi instance and
                                // not some other location
                                return dataRef.startsWith(about.uri);
                            }

                            return false;
                        }),
                        map(() => queryParams)
                    );
                })
            )
            .subscribe((queryParams) => {
                const ref = queryParams['ref'];
                this.mimeType = queryParams['mimeType'];
                this.clientId = queryParams['clientId'];

                if (ref) {
                    this.store.dispatch(
                        setRef({
                            ref
                        })
                    );
                }

                this.queryParamsLoaded = true;

                if (this.viewerOptionsLoaded) {
                    this.handleDefaultSelection();
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadContentViewerOptions());
        this.store.dispatch(loadAbout());
    }

    private handleDefaultSelection(): void {
        if (!this.viewerSelected) {
            if (this.mimeType) {
                const compatibleViewerOption = this.getCompatibleViewer(this.mimeType);
                if (Number.isNaN(compatibleViewerOption)) {
                    if (!Number.isNaN(this.defaultSupportedMimeTypeId)) {
                        this.viewerForm.get('viewAs')?.setValue(String(this.defaultSupportedMimeTypeId));
                        this.loadBundledContentViewer(this.defaultSupportedMimeTypeId);
                    }
                } else {
                    this.viewerForm.get('viewAs')?.setValue(String(compatibleViewerOption));
                    this.loadBundledContentViewer(compatibleViewerOption);
                }
            } else if (!Number.isNaN(this.defaultSupportedMimeTypeId)) {
                this.viewerForm.get('viewAs')?.setValue(String(this.defaultSupportedMimeTypeId));
                this.loadBundledContentViewer(this.defaultSupportedMimeTypeId);
            }
        }
    }

    private getCompatibleViewer(mimeType: string): number | null {
        for (const group of this.viewAsOptions) {
            for (const option of group.options) {
                const supportedMimeTypeId: number = Number(option.value);
                if (!Number.isNaN(supportedMimeTypeId)) {
                    const supportedMimeType = this.supportedMimeTypeLookup.get(supportedMimeTypeId);
                    if (supportedMimeType) {
                        if (supportedMimeType.mimeTypes.includes(mimeType)) {
                            return supportedMimeTypeId;
                        }
                    }
                }
            }
        }

        return null;
    }

    viewAsChanged(event: MatSelectChange): void {
        this.loadBundledContentViewer(Number(event.value));
    }

    loadBundledContentViewer(value: number | null): void {
        if (value !== null) {
            const viewer = this.supportedMimeTypeContentViewerLookup.get(value);

            if (viewer) {
                this.viewerSelected = true;

                if (this.mimeTypeIdsSupportedByBundledUis.has(value)) {
                    this.store.dispatch(
                        navigateToBundledContentViewer({
                            route: viewer.uri
                        })
                    );
                } else {
                    const mimeTypeDisplayName = this.mimeTypeDisplayNameLookup.get(value);
                    if (mimeTypeDisplayName) {
                        this.store.dispatch(
                            navigateToExternalViewer({
                                request: {
                                    url: viewer.uri,
                                    mimeTypeDisplayName,
                                    clientId: this.clientId
                                }
                            })
                        );
                    }
                }
            }
        }
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetContent());
        this.store.dispatch(resetContentViewerOptions());
    }
}
