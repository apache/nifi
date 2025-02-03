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
import { isDefinedAndNotNull, NiFiCommon, SelectGroup, SelectOption, selectQueryParams, TextTip } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { concatLatestFrom } from '@ngrx/operators';
import { navigateToBundledContentViewer, resetContent, setRef } from '../state/content/content.actions';
import { MatSelectChange } from '@angular/material/select';
import { loadAbout } from '../../../state/about/about.actions';
import { selectAbout } from '../../../state/about/about.selectors';
import { filter, map, switchMap, take } from 'rxjs';
import { navigateToExternalViewer } from '../state/external-viewer/external-viewer.actions';
import { snackBarError } from '../../../state/error/error.actions';

interface SupportedContentViewer {
    supportedMimeTypes: SupportedMimeTypes;
    contentViewer: ContentViewer;
    bundled: boolean;
}

@Component({
    selector: 'content-viewer',
    templateUrl: './content-viewer.component.html',
    styleUrls: ['./content-viewer.component.scss'],
    standalone: false
})
export class ContentViewerComponent implements OnInit, OnDestroy {
    viewerForm: FormGroup;
    viewAsOptions: SelectGroup[] = [];
    panelWidth: string | null = 'auto';

    private supportedMimeTypeId = 0;
    private supportedContentViewerLookup: Map<number, SupportedContentViewer> = new Map<
        number,
        SupportedContentViewer
    >();

    private defaultSupportedMimeTypeId: number | null = null;

    viewerSelected: boolean = false;
    mimeType: string | undefined;
    private clientId: string | undefined;

    private queryParamsLoaded = false;
    private viewerOptionsLoaded = false;

    constructor(
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon,
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
                this.supportedContentViewerLookup.clear();

                // maps a given content (by display name) to the supported mime type id
                // which can be used to look up the corresponding content viewer
                const supportedMimeTypeMapping = new Map<string, number[]>();

                // process all external viewer options
                externalViewerOptions.forEach((contentViewer) => {
                    contentViewer.supportedMimeTypes.forEach((supportedMimeTypes) => {
                        const supportedMimeTypeId = this.supportedMimeTypeId++;

                        if (!supportedMimeTypeMapping.has(supportedMimeTypes.displayName)) {
                            supportedMimeTypeMapping.set(supportedMimeTypes.displayName, []);
                        }
                        supportedMimeTypeMapping.get(supportedMimeTypes.displayName)?.push(supportedMimeTypeId);

                        this.supportedContentViewerLookup.set(supportedMimeTypeId, {
                            supportedMimeTypes,
                            contentViewer,
                            bundled: false
                        });
                    });
                });

                // process all bundled options
                bundledViewerOptions.forEach((contentViewer) => {
                    contentViewer.supportedMimeTypes.forEach((supportedMimeTypes) => {
                        const supportedMimeTypeId = this.supportedMimeTypeId++;

                        if (contentViewer.uri === HEX_VIEWER_URL) {
                            this.defaultSupportedMimeTypeId = supportedMimeTypeId;
                        }

                        if (!supportedMimeTypeMapping.has(supportedMimeTypes.displayName)) {
                            supportedMimeTypeMapping.set(supportedMimeTypes.displayName, []);
                        }
                        supportedMimeTypeMapping.get(supportedMimeTypes.displayName)?.push(supportedMimeTypeId);

                        this.supportedContentViewerLookup.set(supportedMimeTypeId, {
                            supportedMimeTypes,
                            contentViewer,
                            bundled: true
                        });
                    });
                });

                const newViewAsOptions: SelectGroup[] = [];
                supportedMimeTypeMapping.forEach((contentViewers, displayName) => {
                    const options: SelectOption[] = [];
                    contentViewers.forEach((supportedMimeTypeId) => {
                        const supportedContentViewer = this.supportedContentViewerLookup.get(supportedMimeTypeId);
                        if (supportedContentViewer) {
                            const option: SelectOption = {
                                text: supportedContentViewer.contentViewer.displayName,
                                value: String(supportedMimeTypeId)
                            };
                            options.push(option);
                        }
                    });

                    // if there is more than one content viewer for this mime type we sent the panel
                    // width to null which will allow the select menu to expand to the width of
                    // the content which will be a lengthy nar version string
                    if (options.length > 1) {
                        this.panelWidth = null;
                    }

                    // sort by option text
                    options.sort((a, b) => {
                        return this.nifiCommon.compareString(a.text, b.text);
                    });

                    const groupOption: SelectGroup = {
                        text: displayName,
                        options
                    };
                    newViewAsOptions.push(groupOption);
                });

                // sort by option text
                newViewAsOptions.sort((a, b) => {
                    return this.nifiCommon.compareString(a.text, b.text);
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
                if (compatibleViewerOption === null) {
                    if (this.defaultSupportedMimeTypeId !== null) {
                        this.viewerForm.get('viewAs')?.setValue(String(this.defaultSupportedMimeTypeId));
                        this.loadContentViewer(this.defaultSupportedMimeTypeId);
                    }

                    this.store.dispatch(
                        snackBarError({
                            error: `No compatible content viewer found for mime type [${this.mimeType}]`
                        })
                    );
                } else {
                    this.viewerForm.get('viewAs')?.setValue(String(compatibleViewerOption));
                    this.loadContentViewer(compatibleViewerOption);
                }
            } else if (this.defaultSupportedMimeTypeId !== null) {
                this.viewerForm.get('viewAs')?.setValue(String(this.defaultSupportedMimeTypeId));
                this.loadContentViewer(this.defaultSupportedMimeTypeId);
            }
        }
    }

    private getCompatibleViewer(mimeType: string): number | null {
        for (const group of this.viewAsOptions) {
            for (const option of group.options) {
                const supportedMimeTypeId: number = Number(option.value);
                if (Number.isInteger(supportedMimeTypeId)) {
                    const supportedContentViewer = this.supportedContentViewerLookup.get(supportedMimeTypeId);
                    if (supportedContentViewer) {
                        const supportsMimeType = supportedContentViewer.supportedMimeTypes.mimeTypes.some(
                            (supportedMimeType) => mimeType.startsWith(supportedMimeType)
                        );

                        if (supportsMimeType) {
                            return supportedMimeTypeId;
                        }
                    }
                }
            }
        }

        return null;
    }

    viewAsChanged(event: MatSelectChange): void {
        this.loadContentViewer(Number(event.value));
    }

    loadContentViewer(value: number | null): void {
        if (value !== null) {
            const supportedContentViewer = this.supportedContentViewerLookup.get(value);

            if (supportedContentViewer) {
                const viewer = supportedContentViewer.contentViewer;

                this.viewerSelected = true;

                if (supportedContentViewer.bundled) {
                    this.store.dispatch(
                        navigateToBundledContentViewer({
                            route: viewer.uri
                        })
                    );
                } else {
                    this.store.dispatch(
                        navigateToExternalViewer({
                            request: {
                                url: viewer.uri,
                                mimeTypeDisplayName: supportedContentViewer.supportedMimeTypes.displayName,
                                clientId: this.clientId
                            }
                        })
                    );
                }
            }
        }
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetContent());
        this.store.dispatch(resetContentViewerOptions());
    }

    protected readonly TextTip = TextTip;
}
