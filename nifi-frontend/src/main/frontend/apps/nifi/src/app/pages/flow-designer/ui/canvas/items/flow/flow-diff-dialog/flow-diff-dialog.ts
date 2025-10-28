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

import {
    AfterViewInit,
    Component,
    DestroyRef,
    ViewChild,
    inject
} from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { MatFormField, MatLabel } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatOption } from '@angular/material/core';
import { MatInput } from '@angular/material/input';
import { MatButton } from '@angular/material/button';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { combineLatest, of } from 'rxjs';
import { catchError, debounceTime, distinctUntilChanged, filter, map, startWith, switchMap, take } from 'rxjs/operators';
import { FlowComparisonEntity, VersionControlInformation } from '../../../../../state/flow';
import { VersionedFlowSnapshotMetadata } from '../../../../../../../state/shared';
import { RegistryService } from '../../../../../service/registry.service';
import { CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';

export interface FlowDiffDialogData {
    versionControlInformation: VersionControlInformation;
    versions: VersionedFlowSnapshotMetadata[];
    currentVersion: string;
    selectedVersion: string;
}

interface FlowDiffRow {
    componentName: string;
    changeType: string;
    difference: string;
}

@Component({
    selector: 'flow-diff-dialog',
    standalone: true,
    imports: [
        CommonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatSelectModule,
        MatOption,
        MatInput,
        MatButton,
        MatProgressSpinner
    ],
    templateUrl: './flow-diff-dialog.html',
    styleUrl: './flow-diff-dialog.scss'
})
export class FlowDiffDialog extends CloseOnEscapeDialog implements AfterViewInit {
    private data = inject<FlowDiffDialogData>(MAT_DIALOG_DATA);
    private registryService = inject(RegistryService);
    private destroyRef = inject(DestroyRef);
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);
    private store = inject<Store<CanvasState>>(Store);
    private timeOffset = this.store.selectSignal(selectTimeOffset);

    @ViewChild(MatSort)
    set matSort(sort: MatSort | undefined) {
        if (sort) {
            this.dataSource.sort = sort;
        }
    }

    displayedColumns: string[] = ['componentName', 'changeType', 'difference'];
    dataSource: MatTableDataSource<FlowDiffRow> = new MatTableDataSource<FlowDiffRow>();
    comparisonForm: FormGroup;
    filterControl: FormControl<string> = new FormControl<string>('', { nonNullable: true });
    currentVersionControl: FormControl<string>;
    selectedVersionControl: FormControl<string>;

    versionOptions: string[];
    flowName: string;
    comparisonSummary: { version: string; created?: string }[] = [];
    isLoading = false;
    errorMessage: string | null = null;
    noDifferences = false;
    private versionMetadataByVersion: Map<string, VersionedFlowSnapshotMetadata> = new Map();

    constructor() {
        super();
        const versions = this.sortVersions(this.data.versions);
        this.versionOptions = versions.map((version) => version.version);
        this.versionMetadataByVersion = new Map(versions.map((metadata) => [metadata.version, metadata]));
        const vci = this.data.versionControlInformation;
        this.flowName = vci.flowName || vci.flowId;

        this.currentVersionControl = new FormControl<string>(this.data.currentVersion, { nonNullable: true });
        this.selectedVersionControl = new FormControl<string>(this.data.selectedVersion, { nonNullable: true });
        this.comparisonForm = this.formBuilder.group({
            currentVersion: this.currentVersionControl,
            selectedVersion: this.selectedVersionControl
        });

        this.dataSource.filterPredicate = (row: FlowDiffRow, filterTerm: string) => {
            if (!filterTerm) {
                return true;
            }

            const normalizedFilter = filterTerm.toLowerCase();
            return (
                (row.componentName || '').toLowerCase().includes(normalizedFilter) ||
                (row.changeType || '').toLowerCase().includes(normalizedFilter) ||
                (row.difference || '').toLowerCase().includes(normalizedFilter)
            );
        };

        this.dataSource.sortingDataAccessor = (row: FlowDiffRow, property: string) => {
            switch (property) {
                case 'componentName':
                    return (row.componentName || '').toLowerCase();
                case 'changeType':
                    return (row.changeType || '').toLowerCase();
                case 'difference':
                    return (row.difference || '').toLowerCase();
                default:
                    return '';
            }
        };

        this.configureFiltering();
        this.configureComparisonChanges();
        this.setComparisonSummary(this.data.currentVersion, this.data.selectedVersion);
    }

    formatVersionOption(version: string): string {
        const metadata = this.versionMetadataByVersion.get(version);
        const formattedVersion =
            version.length > 5 ? `${version.substring(0, 5)}...` : version;
        const created = metadata ? this.formatTimestamp(metadata) : undefined;
        return created ? `${formattedVersion} (${created})` : formattedVersion;
    }

    ngAfterViewInit(): void {
        // Sorting handled automatically when MatSort is available via the ViewChild setter
    }

    private configureFiltering(): void {
        this.filterControl.valueChanges
            .pipe(
                startWith(this.filterControl.value),
                debounceTime(200),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((value) => {
                const filterTerm = value ? value.trim() : '';
                this.dataSource.filter = filterTerm.toLowerCase();
            });
    }

    private configureComparisonChanges(): void {
        const currentVersion$ = this.currentVersionControl.valueChanges.pipe(startWith(this.currentVersionControl.value));
        const selectedVersion$ = this.selectedVersionControl.valueChanges.pipe(
            startWith(this.selectedVersionControl.value)
        );

        combineLatest([currentVersion$, selectedVersion$])
            .pipe(
                takeUntilDestroyed(this.destroyRef),
                map(([current, selected]) => [current, selected] as [string | null, string | null]),
                filter(([current, selected]) => !!current && !!selected),
                distinctUntilChanged(
                    ([currentA, selectedA], [currentB, selectedB]) =>
                        currentA === currentB && selectedA === selectedB
                ),
                switchMap(([current, selected]) => {
                    this.isLoading = true;
                    this.errorMessage = null;
                    this.noDifferences = false;
                    return this.fetchFlowDiff(current as string, selected as string).pipe(
                        catchError((error) => {
                            this.isLoading = false;
                            this.errorMessage = 'Unable to retrieve version differences.';
                            console.error('Failed to load flow version diff', error);
                            this.dataSource.data = [];
                            this.noDifferences = true;
                            return of(null);
                        })
                    );
                })
            )
            .subscribe((comparison) => {
                if (!comparison) {
                    return;
                }

                this.isLoading = false;
                this.setComparisonSummary(this.currentVersionControl.value, this.selectedVersionControl.value);
                const rows = this.toRows(comparison);
                this.dataSource.data = rows;
                this.noDifferences = rows.length === 0;
            });
    }

    private fetchFlowDiff(versionA: string, versionB: string) {
        this.setComparisonSummary(versionA, versionB);
        const vci = this.data.versionControlInformation;
        const branch = vci.branch ?? null;

        return this.registryService
            .getFlowDiff(vci.registryId, vci.bucketId, vci.flowId, versionA, versionB, branch)
            .pipe(take(1));
    }

    private sortVersions(versions: VersionedFlowSnapshotMetadata[]): VersionedFlowSnapshotMetadata[] {
        return versions
            .slice()
            .sort((a, b) => {
                const timestampA = this.nifiCommon.isDefinedAndNotNull(a.timestamp) ? a.timestamp : 0;
                const timestampB = this.nifiCommon.isDefinedAndNotNull(b.timestamp) ? b.timestamp : 0;
                const timestampComparison = this.nifiCommon.compareNumber(timestampB, timestampA);
                if (timestampComparison !== 0) {
                    return timestampComparison;
                }

                if (this.nifiCommon.isNumber(a.version) && this.nifiCommon.isNumber(b.version)) {
                    return this.nifiCommon.compareNumber(parseInt(b.version, 10), parseInt(a.version, 10));
                }

                return this.nifiCommon.compareString(b.version, a.version);
            });
    }

    private toRows(comparison: FlowComparisonEntity): FlowDiffRow[] {
        if (!comparison || !comparison.componentDifferences) {
            return [];
        }

        const rows: FlowDiffRow[] = [];
        comparison.componentDifferences.forEach((component) => {
            component.differences.forEach((difference) => {
                rows.push({
                    componentName: component.componentName || '',
                    changeType: difference.differenceType,
                    difference: difference.difference
                });
            });
        });

        return rows;
    }

    private setComparisonSummary(versionA: string, versionB: string): void {
        this.comparisonSummary = [versionA, versionB].map((version) => {
            const metadata = this.versionMetadataByVersion.get(version);
            return {
                version,
                created: metadata ? this.formatTimestamp(metadata) : undefined
            };
        });
    }

    private formatTimestamp(metadata: VersionedFlowSnapshotMetadata): string | undefined {
        if (!metadata.timestamp) {
            return undefined;
        }

        const now = new Date();
        const userOffset = now.getTimezoneOffset() * 60 * 1000;
        const date = new Date(metadata.timestamp + userOffset + this.getTimezoneOffset());
        return this.nifiCommon.formatDateTime(date);
    }

    private getTimezoneOffset(): number {
        return this.timeOffset() || 0;
    }
}
