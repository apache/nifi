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

import { computed, inject } from '@angular/core';
import { toObservable } from '@angular/core/rxjs-interop';
import { HttpErrorResponse } from '@angular/common/http';
import { patchState, signalStoreFeature, withComputed, withMethods, withState } from '@ngrx/signals';
import { rxMethod } from '@ngrx/signals/rxjs-interop';
import { tapResponse } from '@ngrx/operators';
import { MatDialog } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';
import {
    Observable,
    BehaviorSubject,
    Subject,
    switchMap,
    exhaustMap,
    tap,
    catchError,
    map,
    filter,
    interval,
    takeUntil,
    mergeMap,
    from,
    take,
    concatMap,
    EMPTY,
    pipe,
    race,
    finalize,
    share
} from 'rxjs';
import { ConnectorConfigurationService } from '../../services/connector-configuration.service';
import { ConnectorVerificationProgressDialog } from '../connector-verification-progress-dialog/connector-verification-progress-dialog.component';
import {
    SMALL_DIALOG,
    ConnectorEntity,
    ConnectorPropertyFormValue,
    ConfigurationStepConfiguration,
    ConfigurationStepEntity,
    ConfigVerificationResult,
    ConnectorConfigStepVerificationRequest,
    ConnectorConfigStepVerificationRequestEntity,
    SecretsEntity,
    AssetInfo,
    AllowableValue,
    StepDocumentationState
} from '../../types';
import {
    ConnectorWizardState,
    CONNECTOR_WIZARD_CONFIG,
    createPropertyKey,
    initialConnectorWizardState
} from './connector-wizard.types';
import { getVisibleStepNames } from './step-dependency.utils';

/**
 * Reusable SignalStore feature encapsulating all shared connector wizard logic.
 *
 * Application-specific stores can compose this feature with additional `withState` /
 * `withComputed` / `withHooks` blocks; later definitions take precedence in SignalStore
 * composition.
 */
export function withConnectorWizard() {
    return signalStoreFeature(
        withState<ConnectorWizardState>(initialConnectorWizardState),

        withComputed(
            ({ stepNames, stepConfigurations, unsavedStepValues, connector, status, allStepsVerification }) => ({
                visibleStepNames: computed(() =>
                    getVisibleStepNames(stepNames(), stepConfigurations(), unsavedStepValues())
                ),
                workingConfiguration: computed(() => connector()?.component?.workingConfiguration ?? null),
                applying: computed(() => status() === 'saving'),
                allStepsVerifying: computed(() => allStepsVerification().verifying),
                verificationPassed: computed(() => allStepsVerification().passed),
                currentVerifyingStepName: computed(() => allStepsVerification().currentStepName),
                verifyAllError: computed(() => allStepsVerification().error)
            })
        ),

        withMethods(
            (
                store,
                connectorConfigService = inject(ConnectorConfigurationService),
                dialog = inject(MatDialog),
                snackBar = inject(MatSnackBar),
                config = inject(CONNECTOR_WIZARD_CONFIG, { optional: true })
            ) => {
                // Created in injection context: toObservable works here.
                const activeVerificationRequest$ = toObservable(computed(() => store.activeVerificationRequest())).pipe(
                    filter((r): r is ConnectorConfigStepVerificationRequest => r !== null)
                );

                // --------------- Private Helpers ---------------

                function showToast(message: string, _variant: 'success' | 'error'): void {
                    snackBar.open(message, 'Dismiss', { duration: 5000 });
                }

                /**
                 * Delete verification request on stop (cleanup) and return results.
                 */
                function cleanupAndReturnResults(
                    connectorId: string,
                    stepName: string,
                    requestId: string
                ): Observable<ConfigVerificationResult[]> {
                    return connectorConfigService
                        .deleteVerificationRequest(connectorId, stepName, requestId)
                        .pipe(map((response) => response.request.results || []));
                }

                /**
                 * Verify a single configuration step (async API).
                 * Submits verification request, polls until complete, then cleans up.
                 */
                function runStepVerification(
                    connectorId: string,
                    stepName: string,
                    stepConfig: ConfigurationStepConfiguration,
                    onProgress: (req: ConnectorConfigStepVerificationRequest) => void
                ): Observable<ConfigVerificationResult[]> {
                    return connectorConfigService.submitVerificationRequest(connectorId, stepName, stepConfig).pipe(
                        tap((response: ConnectorConfigStepVerificationRequestEntity) => onProgress(response.request)),
                        switchMap((response: ConnectorConfigStepVerificationRequestEntity) => {
                            if (response.request.complete) {
                                return cleanupAndReturnResults(connectorId, stepName, response.request.requestId);
                            }
                            return interval(2000).pipe(
                                switchMap(() =>
                                    connectorConfigService.getVerificationRequest(
                                        connectorId,
                                        stepName,
                                        response.request.requestId
                                    )
                                ),
                                tap((r) => onProgress(r.request)),
                                filter((r) => r.request.complete),
                                take(1),
                                switchMap((r) => cleanupAndReturnResults(connectorId, stepName, r.request.requestId))
                            );
                        })
                    );
                }

                /**
                 * Handle verification results - split errors between field-level (with subject)
                 * and banner (without subject). All successful = show success toast.
                 */
                function processVerificationResults(stepName: string, results: ConfigVerificationResult[]): void {
                    const failedResults = results.filter((r) => r.outcome === 'FAILED');

                    if (failedResults.length > 0) {
                        const subjectErrors = failedResults.filter(
                            (r): r is ConfigVerificationResult & { subject: string } => !!r.subject
                        );
                        if (subjectErrors.length > 0) {
                            patchState(store, {
                                subjectVerificationErrors: subjectErrors.reduce(
                                    (acc, r) => ({ ...acc, [r.subject]: r.explanation }),
                                    {} as Record<string, string>
                                )
                            });
                        } else {
                            patchState(store, { subjectVerificationErrors: {} });
                        }
                        patchState(store, {
                            stepVerificationResults: {
                                ...store.stepVerificationResults(),
                                [stepName]: failedResults
                            }
                        });
                    } else {
                        showToast('Verification passed successfully', 'success');
                        patchState(store, {
                            stepVerificationResults: {
                                ...store.stepVerificationResults(),
                                [stepName]: []
                            },
                            subjectVerificationErrors: {}
                        });
                    }
                }

                /**
                 * Fetch secrets from the API.
                 * Secrets are loaded after connector data and made available for all SECRET type properties.
                 */
                function fetchSecrets(connectorId: string): Observable<unknown> {
                    patchState(store, { secretsLoading: true, secretsError: null });
                    return connectorConfigService.getSecrets(connectorId).pipe(
                        tapResponse({
                            next: (response: SecretsEntity) => {
                                patchState(store, {
                                    availableSecrets: response.secrets,
                                    secretsLoading: false,
                                    secretsError: null
                                });
                            },
                            error: (error: HttpErrorResponse) => {
                                patchState(store, {
                                    secretsLoading: false,
                                    secretsError: error.error?.message || error.message || 'Failed to load secrets'
                                });
                            }
                        })
                    );
                }

                /**
                 * Coordinate step advancement after successful save.
                 * Updates step configuration from save response, marks step as completed,
                 * clears unsaved values, and requests step advancement.
                 */
                function onSaveStepSuccess(stepName: string, stepEntity: ConfigurationStepEntity): void {
                    const { [stepName]: _removed, ...remainingUnsaved } = store.unsavedStepValues();
                    patchState(store, {
                        parentConnectorRevision: stepEntity.parentConnectorRevision,
                        stepConfigurations: {
                            ...store.stepConfigurations(),
                            [stepName]: stepEntity.configurationStep
                        },
                        completedSteps: { ...store.completedSteps(), [stepName]: true },
                        savingSteps: { ...store.savingSteps(), [stepName]: false },
                        dirtySteps: { ...store.dirtySteps(), [stepName]: false },
                        unsavedStepValues: remainingUnsaved,
                        status: 'configuring'
                    });
                    patchState(store, { pendingStepAdvancement: true });
                }

                /**
                 * Show banner error when a configuration step save fails.
                 */
                function onSaveStepFailure(stepName: string, error: string): void {
                    const { [stepName]: _removed, ...remainingUnsaved } = store.unsavedStepValues();
                    patchState(store, {
                        savingSteps: { ...store.savingSteps(), [stepName]: false },
                        unsavedStepValues: remainingUnsaved,
                        status: 'configuring',
                        bannerErrors: [...store.bannerErrors(), error]
                    });
                }

                // --------------- Returned methods ---------------

                return {
                    // --- Per-step signal factories (computed signals keyed by step name) ---
                    stepConfiguration(stepName: string) {
                        return computed(() => store.stepConfigurations()[stepName] ?? null);
                    },

                    stepSaving(stepName: string) {
                        return computed(() => store.savingSteps()[stepName] ?? false);
                    },

                    stepCompleted(stepName: string) {
                        return computed(() => store.completedSteps()[stepName] ?? false);
                    },

                    documentationForStep(stepName: string) {
                        return computed(
                            () => (store.stepDocumentation()[stepName] as StepDocumentationState | null) ?? null
                        );
                    },

                    generalVerificationErrorsForStep(stepName: string) {
                        return computed(() => {
                            const results = store.stepVerificationResults()[stepName];
                            if (!results) return [] as string[];
                            return results
                                .filter((r) => r.outcome === 'FAILED' && !r.subject)
                                .map((r) => r.explanation || 'Verification failed');
                        });
                    },

                    // --- State mutation methods (synchronous state updates) ---

                    /**
                     * Initialize wizard with pre-loaded connector.
                     * Populates stepNames and stepConfigurations from workingConfiguration
                     * to enable immediate step dependency evaluation without additional API calls.
                     */
                    initializeWithConnector(connector: ConnectorEntity): void {
                        const stepConfigs =
                            connector.component?.workingConfiguration?.configurationStepConfigurations || [];
                        const stepNames = stepConfigs.map((s) => s.configurationStepName);
                        patchState(store, {
                            ...initialConnectorWizardState,
                            connectorId: connector.id,
                            connector,
                            parentConnectorRevision: connector.revision,
                            stepNames,
                            stepConfigurations: stepConfigs.reduce(
                                (acc, s) => ({ ...acc, [s.configurationStepName]: s }),
                                {} as Record<string, ConfigurationStepConfiguration>
                            ),
                            status: stepNames.length > 0 ? ('configuring' as const) : ('pending' as const),
                            loading: false
                        });
                    },

                    updateConnector(connector: ConnectorEntity): void {
                        patchState(store, {
                            connector,
                            parentConnectorRevision: connector.revision,
                            loading: false
                        });
                    },

                    resetState(): void {
                        patchState(store, { ...initialConnectorWizardState });
                    },

                    changeStep(stepIndex: number): void {
                        patchState(store, { currentStepIndex: stepIndex, bannerErrors: [] });
                    },

                    clearPendingStepAdvancement(): void {
                        patchState(store, { pendingStepAdvancement: false });
                    },

                    markStepDirty(data: { stepName: string; isDirty: boolean }): void {
                        patchState(store, {
                            dirtySteps: { ...store.dirtySteps(), [data.stepName]: data.isDirty },
                            completedSteps: { ...store.completedSteps(), [data.stepName]: false },
                            allStepsVerification: {
                                ...store.allStepsVerification(),
                                passed: null
                            }
                        });
                    },

                    /**
                     * Update unsaved form values for a step.
                     * Recomputes visible steps before and after the change. If visibility changed,
                     * invalidates completedSteps for all steps after the modified step.
                     */
                    updateUnsavedStepValues(data: {
                        stepName: string;
                        values: Record<string, ConnectorPropertyFormValue>;
                    }): void {
                        const currentUnsaved = store.unsavedStepValues();
                        const currentStepNames = store.stepNames();
                        const currentConfigs = store.stepConfigurations();

                        // Compute visible steps BEFORE the change
                        const visibleBefore = getVisibleStepNames(currentStepNames, currentConfigs, currentUnsaved);
                        const newUnsaved = { ...currentUnsaved, [data.stepName]: data.values };
                        // Compute visible steps AFTER the change
                        const visibleAfter = getVisibleStepNames(currentStepNames, currentConfigs, newUnsaved);

                        // Determine if visibility changed
                        const visibilityChanged =
                            visibleBefore.length !== visibleAfter.length ||
                            visibleBefore.some((s, i) => s !== visibleAfter[i]);

                        // Start with marking current step as incomplete (has unsaved values)
                        const newCompleted = { ...store.completedSteps(), [data.stepName]: false };
                        if (visibilityChanged) {
                            // If visibility changed, mark all steps after the modified step as incomplete
                            const modifiedIdx = visibleAfter.indexOf(data.stepName);
                            for (let i = modifiedIdx + 1; i < visibleAfter.length; i++) {
                                newCompleted[visibleAfter[i]] = false;
                            }
                        }

                        patchState(store, {
                            unsavedStepValues: newUnsaved,
                            completedSteps: newCompleted,
                            allStepsVerification: {
                                ...store.allStepsVerification(),
                                passed: null
                            }
                        });
                    },

                    clearUnsavedStepValues(stepName: string): void {
                        const { [stepName]: _removed, ...remaining } = store.unsavedStepValues();
                        patchState(store, { unsavedStepValues: remaining });
                    },

                    setLoading(loading: boolean): void {
                        patchState(store, { loading });
                    },

                    toggleDocumentationPanel(open: boolean): void {
                        patchState(store, { documentationPanelOpen: open });
                    },

                    clearBannerErrors(): void {
                        patchState(store, { bannerErrors: [] });
                    },

                    addBannerError(message: string): void {
                        patchState(store, { bannerErrors: [...store.bannerErrors(), message] });
                    },

                    clearSubjectVerificationError(propertyName: string): void {
                        const { [propertyName]: _removed, ...remaining } = store.subjectVerificationErrors();
                        patchState(store, { subjectVerificationErrors: remaining });
                    },

                    setStepVerificationResults(data: { stepName: string; results: ConfigVerificationResult[] }): void {
                        patchState(store, {
                            stepVerificationResults: {
                                ...store.stepVerificationResults(),
                                [data.stepName]: data.results
                            }
                        });
                    },

                    enterSummaryStep(): void {
                        patchState(store, {
                            allStepsVerification: {
                                verifying: false,
                                passed: null,
                                currentStepName: null,
                                failedStepName: null,
                                error: null
                            },
                            stepVerificationResults: {},
                            subjectVerificationErrors: {}
                        });
                    },

                    addAsset(data: { propertyName: string; asset: AssetInfo; uploadId: number }): void {
                        if (store.processedUploadIds().includes(data.uploadId)) return;
                        const currentAssets = store.assetsByProperty()[data.propertyName] || [];
                        if (currentAssets.some((a) => a.id === data.asset.id)) return;
                        patchState(store, {
                            assetsByProperty: {
                                ...store.assetsByProperty(),
                                [data.propertyName]: [...currentAssets, data.asset]
                            },
                            processedUploadIds: [...store.processedUploadIds(), data.uploadId]
                        });
                    },

                    removeAsset(data: { propertyName: string; assetId: string }): void {
                        const currentAssets = store.assetsByProperty()[data.propertyName] || [];
                        patchState(store, {
                            assetsByProperty: {
                                ...store.assetsByProperty(),
                                [data.propertyName]: currentAssets.filter((a) => a.id !== data.assetId)
                            }
                        });
                    },

                    clearAssetsForProperty(propertyName: string): void {
                        patchState(store, {
                            assetsByProperty: { ...store.assetsByProperty(), [propertyName]: [] }
                        });
                    },

                    setDisconnectedNodeAcknowledged(ack: boolean): void {
                        patchState(store, { disconnectedNodeAcknowledged: ack });
                    },

                    initializeAssets(propertyAssets: Record<string, AssetInfo[]>): void {
                        patchState(store, { assetsByProperty: { ...propertyAssets } });
                    },

                    // --- Effect methods (async operations using rxMethod) ---

                    /** Load connector and automatically fetch secrets on success. */
                    loadConnector: rxMethod<string>(
                        pipe(
                            switchMap((connectorId) => {
                                patchState(store, { loading: true });
                                return connectorConfigService.getConnector(connectorId).pipe(
                                    tapResponse({
                                        next: (connector: ConnectorEntity) => {
                                            const stepConfigs =
                                                connector.component?.workingConfiguration
                                                    ?.configurationStepConfigurations || [];
                                            const stepNames = stepConfigs.map((s) => s.configurationStepName);
                                            patchState(store, {
                                                ...initialConnectorWizardState,
                                                connectorId: connector.id,
                                                connector,
                                                parentConnectorRevision: connector.revision,
                                                stepNames,
                                                stepConfigurations: stepConfigs.reduce(
                                                    (acc, s) => ({
                                                        ...acc,
                                                        [s.configurationStepName]: s
                                                    }),
                                                    {} as Record<string, ConfigurationStepConfiguration>
                                                ),
                                                status:
                                                    stepNames.length > 0
                                                        ? ('configuring' as const)
                                                        : ('pending' as const),
                                                loading: false
                                            });
                                        },
                                        error: (error: HttpErrorResponse) => {
                                            patchState(store, {
                                                status: 'error',
                                                error:
                                                    error.error?.message || error.message || 'Failed to load connector',
                                                loading: false
                                            });
                                        }
                                    }),
                                    switchMap(() => {
                                        const id = store.connectorId();
                                        return id ? fetchSecrets(id) : EMPTY;
                                    })
                                );
                            })
                        )
                    ),

                    /** Load secrets when connector is already provided via input. */
                    loadSecrets: rxMethod<string>(pipe(switchMap((connectorId) => fetchSecrets(connectorId)))),

                    /**
                     * Save configuration step - directly dispatches success on API response (no polling).
                     * Uses exhaustMap to prevent concurrent saves from canceling each other.
                     */
                    saveStep: rxMethod<{
                        stepName: string;
                        stepConfiguration: ConfigurationStepConfiguration;
                    }>(
                        pipe(
                            exhaustMap((data) => {
                                const connectorId = store.connectorId();
                                const parentConnectorRevision = store.parentConnectorRevision();
                                const disconnected =
                                    (config?.getDisconnectedNodeAcknowledged?.() ?? false) ||
                                    store.disconnectedNodeAcknowledged();

                                if (!connectorId || !parentConnectorRevision) {
                                    onSaveStepFailure(data.stepName, 'No connector ID available');
                                    return EMPTY;
                                }

                                patchState(store, {
                                    status: 'saving',
                                    savingSteps: {
                                        ...store.savingSteps(),
                                        [data.stepName]: true
                                    }
                                });

                                return connectorConfigService
                                    .updateConfigurationStep(
                                        connectorId,
                                        data.stepName,
                                        data.stepConfiguration,
                                        parentConnectorRevision,
                                        disconnected
                                    )
                                    .pipe(
                                        tapResponse({
                                            next: (stepEntity: ConfigurationStepEntity) => {
                                                onSaveStepSuccess(data.stepName, stepEntity);
                                            },
                                            error: (error: HttpErrorResponse) => {
                                                onSaveStepFailure(
                                                    data.stepName,
                                                    error.error?.message || error.message || 'Failed to save step'
                                                );
                                            }
                                        })
                                    );
                            })
                        )
                    ),

                    /**
                     * Save and close - save the current step (if dirty) and navigate back to connector details.
                     * Uses exhaustMap to prevent concurrent saves from canceling each other.
                     */
                    saveAndClose: rxMethod<{
                        stepName: string;
                        stepConfiguration: ConfigurationStepConfiguration | null;
                    }>(
                        pipe(
                            exhaustMap((data) => {
                                // No changes to save - navigate immediately
                                if (!data.stepConfiguration) {
                                    config?.onNavigateBack?.(store.connectorId());
                                    return EMPTY;
                                }

                                const connectorId = store.connectorId();
                                const parentConnectorRevision = store.parentConnectorRevision();
                                if (!connectorId || !parentConnectorRevision) {
                                    onSaveStepFailure(data.stepName, 'No connector ID available');
                                    return EMPTY;
                                }

                                const disconnected =
                                    (config?.getDisconnectedNodeAcknowledged?.() ?? false) ||
                                    store.disconnectedNodeAcknowledged();
                                patchState(store, {
                                    savingSteps: {
                                        ...store.savingSteps(),
                                        [data.stepName]: true
                                    }
                                });

                                return connectorConfigService
                                    .updateConfigurationStep(
                                        connectorId,
                                        data.stepName,
                                        data.stepConfiguration,
                                        parentConnectorRevision,
                                        disconnected
                                    )
                                    .pipe(
                                        tapResponse({
                                            next: () => {
                                                patchState(store, {
                                                    savingSteps: {
                                                        ...store.savingSteps(),
                                                        [data.stepName]: false
                                                    }
                                                });
                                                config?.onNavigateBack?.(connectorId);
                                            },
                                            error: (error: HttpErrorResponse) => {
                                                onSaveStepFailure(
                                                    data.stepName,
                                                    error.error?.message || error.message || 'Failed to save step'
                                                );
                                            }
                                        })
                                    );
                            })
                        )
                    ),

                    /**
                     * Advance step without saving (when form is valid but not dirty).
                     * Marks step as complete and requests step advancement.
                     */
                    advanceWithoutSaving(stepName: string): void {
                        patchState(store, {
                            completedSteps: { ...store.completedSteps(), [stepName]: true },
                            pendingStepAdvancement: true
                        });
                    },

                    /**
                     * Verify a single configuration step (async API).
                     * Submits verification request, opens progress dialog, and polls until complete.
                     * Clears all errors when verification starts to avoid confusion with stale errors.
                     */
                    verifyStep: rxMethod<{
                        stepName: string;
                        configuration: ConfigurationStepConfiguration;
                    }>(
                        pipe(
                            switchMap((data) => {
                                const connectorId = store.connectorId();
                                if (!connectorId) return EMPTY;

                                // Clear all errors when verification starts
                                patchState(store, {
                                    verifying: true,
                                    activeVerificationRequest: null,
                                    bannerErrors: [],
                                    subjectVerificationErrors: {},
                                    stepVerificationResults: {
                                        ...store.stepVerificationResults(),
                                        [data.stepName]: []
                                    }
                                });

                                const dialogRef = dialog.open(ConnectorVerificationProgressDialog, {
                                    ...SMALL_DIALOG
                                });
                                dialogRef.componentInstance.stepName = data.stepName;
                                dialogRef.componentInstance.verificationRequest$ = activeVerificationRequest$;

                                const stopped$ = race(
                                    dialogRef.componentInstance.stopVerification.pipe(take(1)),
                                    dialogRef.afterClosed()
                                ).pipe(take(1), share());

                                return runStepVerification(connectorId, data.stepName, data.configuration, (req) =>
                                    patchState(store, { activeVerificationRequest: req })
                                ).pipe(
                                    tap((results) => processVerificationResults(data.stepName, results)),
                                    catchError((err: HttpErrorResponse) => {
                                        const errMsg = err.error?.message || err.message || 'Verification failed';
                                        patchState(store, {
                                            bannerErrors: [...store.bannerErrors(), errMsg]
                                        });
                                        return EMPTY;
                                    }),
                                    finalize(() => {
                                        patchState(store, {
                                            verifying: false,
                                            activeVerificationRequest: null
                                        });
                                        dialogRef.close();
                                    }),
                                    takeUntil(stopped$)
                                );
                            })
                        )
                    ),

                    /**
                     * Verify all visible steps in sequence.
                     * Stops on first failure or user cancellation.
                     */
                    verifyAllSteps: rxMethod<void>(
                        pipe(
                            switchMap(() => {
                                const connectorId = store.connectorId();
                                if (!connectorId) return EMPTY;

                                const stepNames = store.visibleStepNames();
                                const stepConfigurations = store.stepConfigurations();

                                patchState(store, {
                                    allStepsVerification: {
                                        verifying: true,
                                        passed: null,
                                        currentStepName: null,
                                        failedStepName: null,
                                        error: null
                                    },
                                    subjectVerificationErrors: {},
                                    stepVerificationResults: {},
                                    bannerErrors: []
                                });

                                const progressSubject =
                                    new BehaviorSubject<ConnectorConfigStepVerificationRequest | null>(null);
                                const dialogRef = dialog.open(ConnectorVerificationProgressDialog, {
                                    ...SMALL_DIALOG
                                });
                                dialogRef.componentInstance.verificationRequest$ = progressSubject.pipe(
                                    filter((r): r is ConnectorConfigStepVerificationRequest => r !== null)
                                );

                                progressSubject
                                    .pipe(
                                        filter((r): r is ConnectorConfigStepVerificationRequest => r !== null),
                                        takeUntil(dialogRef.afterClosed())
                                    )
                                    .subscribe((r) => {
                                        dialogRef.componentInstance.stepName = r.configurationStepName;
                                    });

                                const cancel$ = new Subject<void>();
                                let cancelRequested = false;
                                dialogRef.componentInstance.stopVerification
                                    .pipe(take(1), takeUntil(dialogRef.afterClosed()))
                                    .subscribe(() => {
                                        cancelRequested = true;
                                        cancel$.next();
                                        cancel$.complete();
                                    });

                                let failed = false;
                                let httpError = false;

                                return from(stepNames).pipe(
                                    concatMap((stepName) => {
                                        if (failed || cancelRequested) return EMPTY;

                                        const stepConfig = stepConfigurations[stepName];
                                        if (!stepConfig) return EMPTY;

                                        patchState(store, {
                                            allStepsVerification: {
                                                ...store.allStepsVerification(),
                                                currentStepName: stepName
                                            }
                                        });

                                        return runStepVerification(connectorId, stepName, stepConfig, (req) =>
                                            progressSubject.next(req)
                                        ).pipe(
                                            takeUntil(cancel$),
                                            concatMap((results) => {
                                                const failedResults = results.filter((r) => r.outcome === 'FAILED');
                                                if (failedResults.length > 0) {
                                                    failed = true;
                                                    const subjectErrors = failedResults.filter(
                                                        (
                                                            r
                                                        ): r is ConfigVerificationResult & {
                                                            subject: string;
                                                        } => !!r.subject
                                                    );
                                                    if (subjectErrors.length > 0) {
                                                        patchState(store, {
                                                            subjectVerificationErrors: subjectErrors.reduce(
                                                                (acc, r) => ({
                                                                    ...acc,
                                                                    [r.subject]: r.explanation
                                                                }),
                                                                {} as Record<string, string>
                                                            )
                                                        });
                                                    }
                                                    patchState(store, {
                                                        stepVerificationResults: {
                                                            ...store.stepVerificationResults(),
                                                            [stepName]: failedResults
                                                        },
                                                        allStepsVerification: {
                                                            verifying: false,
                                                            passed: false,
                                                            currentStepName: null,
                                                            failedStepName: stepName,
                                                            error: null
                                                        }
                                                    });
                                                    return EMPTY;
                                                }
                                                patchState(store, {
                                                    stepVerificationResults: {
                                                        ...store.stepVerificationResults(),
                                                        [stepName]: []
                                                    }
                                                });
                                                return EMPTY;
                                            }),
                                            catchError((error: HttpErrorResponse) => {
                                                failed = true;
                                                httpError = true;
                                                patchState(store, {
                                                    allStepsVerification: {
                                                        verifying: false,
                                                        passed: false,
                                                        currentStepName: null,
                                                        failedStepName: null,
                                                        error:
                                                            error.error?.message ||
                                                            error.message ||
                                                            'Verification failed'
                                                    }
                                                });
                                                return EMPTY;
                                            })
                                        );
                                    }),
                                    tap({
                                        complete: () => {
                                            dialogRef.close();
                                            progressSubject.complete();
                                            if (!failed && !cancelRequested) {
                                                patchState(store, {
                                                    allStepsVerification: {
                                                        verifying: false,
                                                        passed: true,
                                                        currentStepName: null,
                                                        failedStepName: null,
                                                        error: null
                                                    }
                                                });
                                                showToast('Verification passed successfully', 'success');
                                            } else if (failed && !httpError) {
                                                showToast('Verification encountered errors', 'error');
                                            }
                                            if (cancelRequested) {
                                                patchState(store, {
                                                    allStepsVerification: {
                                                        ...store.allStepsVerification(),
                                                        verifying: false,
                                                        currentStepName: null
                                                    }
                                                });
                                            }
                                        }
                                    })
                                );
                            })
                        )
                    ),

                    /** Handle apply action from summary step - triggers the connector update. */
                    applyConfiguration: rxMethod<void>(
                        pipe(
                            switchMap(() => {
                                const connector = store.connector();
                                if (!connector) return EMPTY;

                                const disconnected =
                                    (config?.getDisconnectedNodeAcknowledged?.() ?? false) ||
                                    store.disconnectedNodeAcknowledged();
                                const connectorId = store.connectorId();

                                patchState(store, { status: 'saving' });

                                return connectorConfigService
                                    .applyConnectorUpdate(connector.id, connector.revision, disconnected)
                                    .pipe(
                                        tapResponse({
                                            next: (response: ConnectorEntity) => {
                                                patchState(store, {
                                                    connector: response,
                                                    status: 'complete'
                                                });
                                                config?.onApplySuccess?.(connectorId);
                                            },
                                            error: (error: HttpErrorResponse) => {
                                                const errorMsg =
                                                    error.error?.message ||
                                                    error.message ||
                                                    'Failed to apply configuration';
                                                patchState(store, {
                                                    status: 'error',
                                                    error: errorMsg,
                                                    loading: false
                                                });
                                            }
                                        })
                                    );
                            })
                        )
                    ),

                    /**
                     * Refresh connector when arriving at the summary step.
                     * Fetches latest connector data to ensure summary displays current state.
                     */
                    refreshConnectorForSummary: rxMethod<void>(
                        pipe(
                            switchMap(() => {
                                const connectorId = store.connectorId();
                                if (!connectorId) return EMPTY;

                                patchState(store, { loading: true });
                                return connectorConfigService.getConnector(connectorId).pipe(
                                    tapResponse({
                                        next: (connector: ConnectorEntity) => {
                                            patchState(store, {
                                                connector,
                                                parentConnectorRevision: connector.revision,
                                                loading: false
                                            });
                                        },
                                        error: (error: HttpErrorResponse) => {
                                            patchState(store, {
                                                status: 'error',
                                                error:
                                                    error.error?.message ||
                                                    error.message ||
                                                    'Failed to refresh connector',
                                                loading: false
                                            });
                                        }
                                    })
                                );
                            })
                        )
                    ),

                    /**
                     * Load step documentation when requested.
                     * Fetches markdown documentation for a configuration step from the flow API.
                     */
                    loadStepDocumentation: rxMethod<string>(
                        pipe(
                            switchMap((stepName) => {
                                const existingDoc = store.stepDocumentation()[stepName];
                                if (existingDoc?.stepDocumentation || existingDoc?.loading || existingDoc?.loaded) {
                                    return EMPTY;
                                }

                                const connector = store.connector();
                                const bundle = connector?.component?.bundle;
                                const type = connector?.component?.type;
                                const metadata = config?.connectorMetadata;

                                const group = metadata?.group ?? bundle?.group;
                                const artifact = metadata?.artifact ?? bundle?.artifact;
                                const version = metadata?.version ?? bundle?.version;
                                const connectorType = metadata?.type ?? type;

                                if (!group || !artifact || !version || !connectorType) return EMPTY;

                                patchState(store, {
                                    stepDocumentation: {
                                        ...store.stepDocumentation(),
                                        [stepName]: {
                                            loading: true,
                                            error: null,
                                            stepDocumentation: null
                                        }
                                    }
                                });

                                return connectorConfigService
                                    .getStepDocumentation(group, artifact, version, connectorType, stepName)
                                    .pipe(
                                        tapResponse({
                                            next: (response: { stepDocumentation: string }) => {
                                                patchState(store, {
                                                    stepDocumentation: {
                                                        ...store.stepDocumentation(),
                                                        [stepName]: {
                                                            loading: false,
                                                            error: null,
                                                            stepDocumentation: response.stepDocumentation,
                                                            loaded: true
                                                        }
                                                    }
                                                });
                                            },
                                            error: (error: HttpErrorResponse) => {
                                                patchState(store, {
                                                    stepDocumentation: {
                                                        ...store.stepDocumentation(),
                                                        [stepName]: {
                                                            loading: false,
                                                            error:
                                                                error.error?.message ||
                                                                error.message ||
                                                                'Failed to load documentation',
                                                            stepDocumentation: null
                                                        }
                                                    }
                                                });
                                            }
                                        })
                                    );
                            })
                        )
                    ),

                    /** Fetch dynamic allowable values for a property. */
                    fetchPropertyAllowableValues: rxMethod<{
                        stepName: string;
                        propertyGroupName: string;
                        propertyName: string;
                        filter?: string;
                    }>(
                        pipe(
                            mergeMap((data) => {
                                const connectorId = store.connectorId();
                                if (!connectorId) return EMPTY;

                                const key = createPropertyKey(data.stepName, data.propertyGroupName, data.propertyName);
                                patchState(store, {
                                    dynamicAllowableValues: {
                                        ...store.dynamicAllowableValues(),
                                        [key]: {
                                            loading: true,
                                            error: null,
                                            values: store.dynamicAllowableValues()[key]?.values ?? null
                                        }
                                    }
                                });

                                return connectorConfigService
                                    .getPropertyAllowableValues(
                                        connectorId,
                                        data.stepName,
                                        data.propertyGroupName,
                                        data.propertyName,
                                        data.filter
                                    )
                                    .pipe(
                                        tapResponse({
                                            next: (response: { allowableValues: AllowableValue[] }) => {
                                                patchState(store, {
                                                    dynamicAllowableValues: {
                                                        ...store.dynamicAllowableValues(),
                                                        [key]: {
                                                            loading: false,
                                                            error: null,
                                                            values: response.allowableValues
                                                        }
                                                    }
                                                });
                                            },
                                            error: (error: HttpErrorResponse) => {
                                                patchState(store, {
                                                    dynamicAllowableValues: {
                                                        ...store.dynamicAllowableValues(),
                                                        [key]: {
                                                            loading: false,
                                                            error:
                                                                error.error?.message ||
                                                                error.message ||
                                                                'Failed to fetch allowable values',
                                                            values: null
                                                        }
                                                    }
                                                });
                                            }
                                        })
                                    );
                            })
                        )
                    )
                };
            }
        )
    );
}
