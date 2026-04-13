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
    Component,
    computed,
    DestroyRef,
    input,
    OnDestroy,
    OnInit,
    output,
    inject,
    ChangeDetectorRef,
    viewChild,
    Injector,
    Signal
} from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators, ValidatorFn } from '@angular/forms';
import { HttpErrorResponse, HttpEvent, HttpEventType } from '@angular/common/http';
import { Observable, Subscription } from 'rxjs';
import { MatButton, MatIconButton } from '@angular/material/button';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { ActiveStepService, SaveableStep } from '../../../services/active-step.service';
import { ConnectorStepActions } from '../../connector-step-actions/connector-step-actions.component';
import { NifiSpinnerDirective } from '../../../directives/spinner/nifi-spinner.directive';
import { SidePanelContainerComponent } from '../../side-panel-container/side-panel-container.component';
import { UploadService } from '../../../services/upload.service';
import { ConnectorConfigurationService } from '../../../services/connector-configuration.service';
import { ConnectorPropertyInput } from '../../connector-property-input/connector-property-input.component';
import { ErrorBanner } from '../../error-banner/error-banner.component';
import { fromValueReference, toValueReference, SecretReferenceOptions } from '../../../services/value-reference.helper';
import {
    AssetInfo,
    AssetReference,
    ConnectorPropertyDescriptor,
    ConnectorPropertyDependency,
    ConnectorPropertyFormValue,
    ConnectorValueReference,
    UploadProgressInfo,
    ConfigurationStepConfiguration,
    parseSecretKey,
    PropertyAllowableValuesState
} from '../../../types';
import { ConnectorWizardStore } from '../connector-wizard.store';
import { WizardContextBanner } from '../wizard-context-banner/wizard-context-banner.component';
import { WizardStepDocumentationPanel } from '../wizard-step-documentation-panel/wizard-step-documentation-panel.component';

@Component({
    selector: 'shared-connector-configuration-step',
    standalone: true,
    imports: [
        ReactiveFormsModule,
        MatButton,
        MatIconButton,
        ConnectorStepActions,
        NifiSpinnerDirective,
        SidePanelContainerComponent,
        ConnectorPropertyInput,
        WizardContextBanner,
        WizardStepDocumentationPanel,
        MatProgressSpinner,
        ErrorBanner
    ],
    templateUrl: './connector-configuration-step.component.html',
    styleUrls: ['./connector-configuration-step.component.scss']
})
export class SharedConnectorConfigurationStep implements SaveableStep, OnInit, OnDestroy {
    private fb = inject(FormBuilder);
    private cdr = inject(ChangeDetectorRef);
    private destroyRef = inject(DestroyRef);
    private injector = inject(Injector);
    private uploadService = inject(UploadService);
    private connectorConfigService = inject(ConnectorConfigurationService);
    private activeStepService = inject(ActiveStepService, { optional: true });
    protected wizardStore = inject(ConnectorWizardStore);

    private sidePanelContainer = viewChild.required<SidePanelContainerComponent>('sidePanelContainer');

    // Connector ID from store (used for asset uploads)
    connectorId = this.wizardStore.connectorId;

    // Documentation panel state — only allow panel to be open if documentation is available
    private documentationPanelOpenFromStore = this.wizardStore.documentationPanelOpen;
    documentationPanelOpen = computed(() => this.hasDocumentation() && this.documentationPanelOpenFromStore());

    // Available secrets for SECRET type properties
    availableSecrets = this.wizardStore.availableSecrets;
    secretsLoading = this.wizardStore.secretsLoading;
    secretsError = this.wizardStore.secretsError;

    stepConfiguration!: Signal<ConfigurationStepConfiguration | null>;
    isStepSaving!: Signal<boolean>;
    isLoading!: Signal<boolean>;
    generalVerificationErrors!: Signal<string[]>;

    readonly isVerifying = computed(() => this.wizardStore.verifying() || this.wizardStore.allStepsVerifying());

    // Signal for reactive access to subject verification errors (field-level errors with subject property)
    private subjectVerificationErrorsSignal = this.wizardStore.subjectVerificationErrors;
    // Dynamic allowable values state per property
    private dynamicAllowableValuesSignal = this.wizardStore.dynamicAllowableValues;
    // Assets from store (keyed by property name)
    private assetsByPropertySignal = this.wizardStore.assetsByProperty;

    readonly stepName = input.required<string>();
    readonly isFirstStep = input(false);

    readonly back = output<{
        stepName: string;
        configuration: ConfigurationStepConfiguration | null;
    }>();

    stepForm!: FormGroup;
    formReady = false;

    // Upload progress tracking (local as it's transient UI state)
    uploadProgressByProperty: Map<string, UploadProgressInfo[]> = new Map();
    private activeUploadSubscriptions: Subscription[] = [];

    // Cache for property visibility to prevent re-evaluation during render cycles
    private propertyVisibilityCache = new Map<string, boolean>();
    // Track if subscription is set up for the current form instance
    private currentFormSubscription: Subscription | null = null;
    // Track previous form values to detect which specific fields changed
    private previousFormValues: Record<string, ConnectorPropertyFormValue> = {};

    private initializeStepSignals(name: string): void {
        this.stepConfiguration = this.wizardStore.stepConfiguration(name);
        this.isStepSaving = this.wizardStore.stepSaving(name);
        this.isLoading = computed(() => !this.stepConfiguration() || this.secretsLoading());
        this.generalVerificationErrors = this.wizardStore.generalVerificationErrorsForStep(name);
    }

    ngOnInit(): void {
        this.initializeStepSignals(this.stepName());
        this.initializeForm();

        // Register this step with the ActiveStepService so the wizard parent
        // can access the current step's configuration for save-on-header-click.
        this.activeStepService?.register(this);

        // Scroll to the top of the step content when a save failure occurs so the
        // inline error banner is visible even if the user has scrolled down.
        toObservable(this.wizardStore.bannerErrors, { injector: this.injector })
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((errors) => {
                if (errors.length > 0) {
                    this.sidePanelContainer().scrollToTop();
                }
            });

        // Subscribe to verification error changes and trigger form revalidation.
        // Validators read directly from the store signal so no local copy is needed.
        toObservable(this.wizardStore.subjectVerificationErrors, { injector: this.injector })
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe(() => {
                this.updateFormValidityForVerificationErrors();
            });

        // Subscribe to asset state changes and sync form controls
        // This handles when the store adds assets after upload completion
        toObservable(this.wizardStore.assetsByProperty, { injector: this.injector })
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((assetsByProperty) => {
                this.syncFormControlsWithAssets(assetsByProperty);
            });
    }

    /**
     * Sync form control values with the current asset state from the store.
     * Called whenever assets change (due to upload completion or initialization).
     */
    private syncFormControlsWithAssets(assetsByProperty: { [propertyName: string]: AssetInfo[] }): void {
        const config = this.stepConfiguration?.();
        if (!config || !this.formReady) return;

        for (const propertyName of Object.keys(assetsByProperty)) {
            const control = this.stepForm.get(propertyName);
            if (!control) continue;

            const property = this.findPropertyDescriptor(propertyName);
            if (!property) continue;

            const assets = assetsByProperty[propertyName] || [];

            if (property.type === 'ASSET') {
                // Single asset - set to first asset ID or null
                const newValue = assets.length > 0 ? assets[0].id : null;
                if (control.value !== newValue) {
                    control.setValue(newValue, { emitEvent: false });
                    control.markAsDirty();
                    this.cdr.detectChanges();
                }
                control.updateValueAndValidity({ emitEvent: false });
            } else if (property.type === 'ASSET_LIST') {
                // Multiple assets - set to array of IDs
                const newValue = assets.map((a) => a.id);
                const currentValue = control.value || [];
                // Only update if values are different
                if (JSON.stringify(currentValue) !== JSON.stringify(newValue)) {
                    control.setValue(newValue, { emitEvent: false });
                    control.markAsDirty();
                    this.cdr.detectChanges();
                }
                control.updateValueAndValidity({ emitEvent: false });
            }
        }
    }

    /**
     * Get property names from the propertyDescriptors object, sorted by dependencies.
     * Properties without dependencies come first, followed by dependent properties.
     * This ensures parent properties appear before their dependents in the UI.
     */
    getPropertyNames(propertyDescriptors: { [key: string]: ConnectorPropertyDescriptor }): string[] {
        const propertyNames = Object.keys(propertyDescriptors || {});
        if (propertyNames.length === 0) return [];

        // Perform topological sort based on dependencies
        return this.sortPropertiesByDependencies(propertyNames, propertyDescriptors);
    }

    /**
     * Sort properties using topological sort to ensure dependencies are respected.
     * Parent properties (those that others depend on) will come before their dependents.
     */
    private sortPropertiesByDependencies(
        propertyNames: string[],
        propertyDescriptors: { [key: string]: ConnectorPropertyDescriptor }
    ): string[] {
        const sorted: string[] = [];
        const visited = new Set<string>();
        const visiting = new Set<string>();

        // Depth-first search with circular dependency detection
        const visit = (propertyName: string): void => {
            if (visited.has(propertyName) || visiting.has(propertyName)) return;

            visiting.add(propertyName);

            const property = propertyDescriptors[propertyName];
            const dependencies = property?.dependencies || [];

            // Visit all dependencies first (parent properties)
            dependencies.forEach((dep: ConnectorPropertyDependency) => {
                const parentPropertyName = dep.propertyName;
                // Only visit if the parent property exists in this group
                if (propertyNames.includes(parentPropertyName)) {
                    visit(parentPropertyName);
                }
            });

            visiting.delete(propertyName);
            visited.add(propertyName);
            sorted.push(propertyName);
        };

        propertyNames.forEach((propertyName) => visit(propertyName));
        return sorted;
    }

    /**
     * Build validators for a property based on its configuration.
     * Note: DOUBLE and FLOAT types use HTML5 type="number" inputs with step="any".
     * INTEGER types use pattern validation to enforce whole numbers only (no decimals).
     */
    private buildValidators(property: ConnectorPropertyDescriptor): ValidatorFn[] {
        const validators: ValidatorFn[] = [];

        if (property.required) {
            validators.push(Validators.required);
        }

        if (property.type === 'ASSET' || property.type === 'ASSET_LIST') {
            validators.push(this.assetContentMissingValidator(property.name));
        }

        if (property.type === 'INTEGER') {
            // Enforce whole numbers only - no decimals allowed
            validators.push(Validators.pattern(/^-?\d+$/));
        }

        // Add verification error validator - checks signal for backend verification errors
        validators.push(this.verificationErrorValidator(property.name));

        return validators;
    }

    /**
     * Creates a validator function that checks for verification errors from the backend.
     * Similar to ControlPlaneBaseForm.apiErrorValidator() - integrates with Angular's
     * validation system so mat-error displays properly.
     *
     * Returns the error message in the validation result so it can be displayed directly.
     * Reads directly from the store signal for synchronous, up-to-date access.
     */
    private verificationErrorValidator(propertyName: string): ValidatorFn {
        return (): { verificationError: string } | null => {
            const errorMessage = this.subjectVerificationErrorsSignal()[propertyName];
            return errorMessage ? { verificationError: errorMessage } : null;
        };
    }

    private assetContentMissingValidator(propertyName: string): ValidatorFn {
        return (): { assetContentMissing: true } | null => {
            const assets = this.assetsByPropertySignal()[propertyName] || [];
            return assets.some((a) => a.missingContent === true) ? { assetContentMissing: true } : null;
        };
    }

    private initializeForm(): void {
        const stepData = this.stepConfiguration?.();
        if (!stepData) {
            this.stepForm = this.fb.group({});
            this.formReady = true;
            return;
        }

        // Clear local progress state when re-initializing
        this.uploadProgressByProperty.clear();

        const unsavedValues: Record<string, ConnectorPropertyFormValue> =
            this.wizardStore.unsavedStepValues()[this.stepName()] ?? {};

        const formConfig: Record<string, [ConnectorPropertyFormValue, ValidatorFn[]]> = {};
        const propertyAssets: { [propertyName: string]: AssetInfo[] } = {};

        stepData.propertyGroupConfigurations?.forEach((group) => {
            Object.keys(group.propertyDescriptors || {}).forEach((propertyName) => {
                const property = group.propertyDescriptors[propertyName];
                // PRIORITY: unsaved values > saved API values > default value > empty string (false for BOOLEAN)
                // For STRING_LIST, default to empty array for multi-select
                // For ASSET types, default to null (single) or empty array (list)
                let fallbackValue: ConnectorPropertyFormValue = '';
                if (property.type === 'BOOLEAN') {
                    fallbackValue = false;
                } else if (property.type === 'STRING_LIST' || property.type === 'ASSET_LIST') {
                    fallbackValue = [];
                } else if (property.type === 'ASSET') {
                    fallbackValue = null;
                }

                // Extract primitive value from ConnectorValueReference for form display
                // Pass property type to handle STRING_LIST comma-separated value conversion
                const apiValue = fromValueReference(group.propertyValues?.[propertyName], property.type);

                // Get the unsaved value, but convert STRING_LIST values from string to array if needed
                // This handles the case where the user previously entered values in a textarea
                // (before allowable values were fetched) and those values were stored as a comma-separated string
                let unsavedValue = unsavedValues[property.name];
                if (property.type === 'STRING_LIST' && typeof unsavedValue === 'string') {
                    unsavedValue = unsavedValue
                        ? unsavedValue
                              .split(',')
                              .map((v: string) => v.trim())
                              .filter((v: string) => v !== '')
                        : [];
                }

                // For ASSET types, collect assets for store state and set form value to asset ID(s)
                if (property.type === 'ASSET' || property.type === 'ASSET_LIST') {
                    const assetValue = (unsavedValue ?? apiValue ?? null) as AssetReference | AssetReference[] | null;
                    const assets = this.buildAssetsFromValue(property.name, property.type, assetValue);
                    if (assets.length > 0) {
                        propertyAssets[property.name] = assets;
                    }

                    // Form control value should be the asset ID (ASSET) or array of IDs (ASSET_LIST)
                    let formValue: string | string[] | null;
                    if (property.type === 'ASSET') {
                        formValue =
                            !Array.isArray(assetValue) && assetValue && typeof assetValue === 'object'
                                ? assetValue.id
                                : null;
                    } else {
                        formValue = Array.isArray(assetValue) ? assetValue.map((a) => a.id) : [];
                    }

                    const validators = this.buildValidators(property);
                    formConfig[property.name] = [formValue, validators];
                } else {
                    const currentValue = unsavedValue ?? apiValue ?? property.defaultValue ?? fallbackValue;
                    const validators = this.buildValidators(property);
                    formConfig[property.name] = [currentValue, validators];
                }
            });
        });

        // Initialize assets in store state with a single call
        this.wizardStore.initializeAssets(propertyAssets);

        this.stepForm = this.fb.group(formConfig);

        // Compute initial visibility for all properties based on current form values
        // This will also disable controls that are initially hidden
        this.computeAllPropertyVisibility();

        stepData.propertyGroupConfigurations?.forEach((group) => {
            Object.keys(group.propertyDescriptors || {}).forEach((propertyName) => {
                const property = group.propertyDescriptors[propertyName];
                const isVisible = this.propertyVisibilityCache.get(property.name);
                const control = this.stepForm.get(property.name);

                if (control && !isVisible) {
                    control.disable({ emitEvent: false });
                }
            });
        });

        // If we loaded unsaved values, mark the form as dirty and the step as dirty
        // This ensures the form will save when user clicks Next
        if (Object.keys(unsavedValues).length > 0) {
            this.stepForm.markAsDirty();
            this.wizardStore.markStepDirty({ stepName: this.stepName(), isDirty: true });
        }

        // Trigger change detection to update template with initial visibility
        this.cdr.detectChanges();

        // Mark form as ready after it's fully built
        this.formReady = true;

        // Set up valueChanges subscription after CVA initialization completes
        // Use microtask queue to ensure all CVAs are fully registered before we subscribe
        Promise.resolve().then(() => {
            this.setupFormSubscription();
        });
    }

    /**
     * Set up form valueChanges subscription after CVA initialization.
     * This ensures all CVA components are properly initialized before we start listening.
     */
    private setupFormSubscription(): void {
        // Clean up any existing subscription for the previous form instance
        if (this.currentFormSubscription) {
            this.currentFormSubscription.unsubscribe();
        }

        // Initialize previous values snapshot
        this.previousFormValues = { ...this.stepForm.getRawValue() };

        // Subscribe to form value changes to update visibility reactively and track dirty state
        this.currentFormSubscription = this.stepForm.valueChanges
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe(() => {
                const currentValues = this.stepForm.getRawValue();

                // Detect which specific fields changed and clear only their errors
                this.clearErrorsForChangedFields(currentValues);

                // Update previous values snapshot
                this.previousFormValues = { ...currentValues };

                this.computeAllPropertyVisibility();
                // Trigger change detection to update template with new visibility
                this.cdr.detectChanges();

                // Mark step as dirty when form values change
                this.wizardStore.markStepDirty({ stepName: this.stepName(), isDirty: true });

                // Store unsaved form values in state to preserve across navigation
                // Use getRawValue() to include disabled controls (hidden properties)
                this.wizardStore.updateUnsavedStepValues({
                    stepName: this.stepName(),
                    values: currentValues
                });
            });
    }

    /**
     * Clear subject verification errors only for fields that have changed.
     * This preserves errors on other fields that haven't been modified.
     */
    private clearErrorsForChangedFields(currentValues: Record<string, ConnectorPropertyFormValue>): void {
        for (const propertyName of Object.keys(currentValues)) {
            if (this.previousFormValues[propertyName] !== currentValues[propertyName]) {
                // Only clear if this property has an error
                if (this.subjectVerificationErrorsSignal()[propertyName]) {
                    this.wizardStore.clearSubjectVerificationError(propertyName);
                    // Trigger revalidation immediately so mat-error clears
                    const control = this.stepForm.get(propertyName);
                    control?.updateValueAndValidity();
                }
            }
        }
    }

    /**
     * Compute visibility for all properties and cache the results.
     * This prevents repeated evaluations during render cycles.
     * Also disables/enables form controls based on visibility to prevent validation
     * on hidden fields.
     */
    private computeAllPropertyVisibility(): void {
        const stepData = this.stepConfiguration?.();
        if (!stepData) return;

        stepData.propertyGroupConfigurations?.forEach((group) => {
            Object.keys(group.propertyDescriptors || {}).forEach((propertyName) => {
                const property = group.propertyDescriptors[propertyName];
                const newVisibility = this.evaluatePropertyVisibility(property);
                const previousVisibility = this.propertyVisibilityCache.get(property.name);

                this.propertyVisibilityCache.set(property.name, newVisibility);

                // Update form control enabled/disabled state based on visibility
                // Hidden controls should be disabled to bypass validation
                const control = this.stepForm.get(property.name);
                if (control) {
                    if (newVisibility && previousVisibility === false) {
                        // Property became visible - enable the control
                        control.enable({ emitEvent: false });
                    } else if (!newVisibility && previousVisibility !== false) {
                        // Property became hidden - disable the control to bypass validation
                        control.disable({ emitEvent: false });
                    }
                }
            });
        });
    }

    /**
     * Find a property descriptor by name across all property groups
     */
    private findPropertyDescriptor(propertyName: string): ConnectorPropertyDescriptor | undefined {
        const stepData = this.stepConfiguration?.();
        if (!stepData?.propertyGroupConfigurations) return undefined;

        for (const group of stepData.propertyGroupConfigurations) {
            if (group.propertyDescriptors?.[propertyName]) {
                return group.propertyDescriptors[propertyName];
            }
        }

        return undefined;
    }

    /**
     * Evaluate whether a property should be visible based on its dependencies.
     * This is the actual visibility logic, separate from the cached getter.
     * Handles transitive dependencies (e.g., C depends on B, B depends on A).
     */
    private evaluatePropertyVisibility(property: ConnectorPropertyDescriptor): boolean {
        if (!property.dependencies || property.dependencies.length === 0) return true;

        return property.dependencies.every((dependency: ConnectorPropertyDependency) => {
            const dependentControl = this.stepForm.get(dependency.propertyName);
            if (!dependentControl) return false;

            const dependentPropertyDescriptor = this.findPropertyDescriptor(dependency.propertyName);
            if (dependentPropertyDescriptor && !this.evaluatePropertyVisibility(dependentPropertyDescriptor)) {
                return false;
            }

            const dependentValue = dependentControl.value;

            if (dependency.dependentValues && dependency.dependentValues.length > 0) {
                // If specific values are required, check if current value matches
                return dependency.dependentValues.includes(dependentValue as string);
            } else {
                // If no specific values, just check if dependent property has any value
                return dependentValue !== null && dependentValue !== undefined && dependentValue !== '';
            }
        });
    }

    /**
     * Get the value of a specific property
     */
    getPropertyValue(propertyName: string): ConnectorPropertyFormValue {
        return this.stepForm.get(propertyName)?.value;
    }

    /**
     * Set the value of a specific property
     */
    setPropertyValue(propertyName: string, value: ConnectorPropertyFormValue): void {
        this.stepForm.get(propertyName)?.setValue(value);
    }

    /**
     * Check if the form has unsaved changes
     */
    isDirty(): boolean {
        return this.stepForm.dirty;
    }

    /**
     * Check if the form is valid
     */
    isValid(): boolean {
        return this.stepForm.valid;
    }

    /**
     * Get all form values
     */
    getFormValue(): Record<string, ConnectorPropertyFormValue> {
        return this.stepForm.value;
    }

    /**
     * Mark the form as pristine (no changes)
     */
    markAsPristine(): void {
        this.stepForm.markAsPristine();
    }

    /**
     * Mark all fields as touched to show validation errors
     */
    markAllAsTouched(): void {
        this.stepForm.markAllAsTouched();
    }

    /**
     * Check if a property should be visible based on its dependencies.
     * Returns the cached visibility state to prevent repeated evaluations during render cycles.
     */
    isPropertyVisible(property: ConnectorPropertyDescriptor): boolean {
        return this.propertyVisibilityCache.get(property.name) ?? true;
    }

    /**
     * Get dynamic allowable values state for a property in a property group.
     * Returns null if no state exists for this property.
     */
    getDynamicAllowableValuesState(
        propertyGroupName: string,
        propertyName: string
    ): PropertyAllowableValuesState | null {
        const key = `${this.stepName()}::${propertyGroupName}::${propertyName}`;
        return this.dynamicAllowableValuesSignal()[key] ?? null;
    }

    /**
     * Handle request to fetch dynamic allowable values for a property.
     * Called when the property input component needs to fetch values.
     */
    onRequestAllowableValues(propertyGroupName: string, propertyName: string): void {
        this.wizardStore.fetchPropertyAllowableValues({
            stepName: this.stepName(),
            propertyGroupName,
            propertyName
        });
    }

    /**
     * Trigger revalidation on form controls that have verification errors.
     * Called when verification errors change to ensure mat-error displays properly.
     */
    private updateFormValidityForVerificationErrors(): void {
        if (!this.stepForm) return;

        Object.keys(this.subjectVerificationErrorsSignal()).forEach((propertyName) => {
            const control = this.stepForm.get(propertyName);
            if (control) {
                // Mark as touched BEFORE updating validity so that ngDoCheck
                // in ConnectorPropertyInput will sync errors to the internal control
                control.markAsTouched();
                control.updateValueAndValidity({ emitEvent: true });
            }
        });

        // Force change detection to ensure property inputs re-sync with parent control errors
        this.cdr.detectChanges();
    }

    /**
     * Check if verify button can be enabled
     */
    get canVerify(): boolean {
        return this.stepForm.valid && !this.isStepSaving() && !this.isVerifying();
    }

    /**
     * Check if save button can be enabled
     */
    get canSave(): boolean {
        return !this.isStepSaving() && !this.isVerifying();
    }

    /**
     * Handle save and close button click.
     * Saves the current step (if dirty) and navigates back to connector details.
     */
    onSaveAndClose(): void {
        this.markAllAsTouched();

        if (!this.stepForm.dirty) {
            // No changes - save with null configuration
            this.wizardStore.saveAndClose({ stepName: this.stepName(), stepConfiguration: null });
            return;
        }

        // Build changed configuration and save
        const configuration = this.buildChangedConfiguration();
        this.wizardStore.saveAndClose({ stepName: this.stepName(), stepConfiguration: configuration });
    }

    /**
     * Handle back button click.
     * Emits back event with the current configuration (if dirty) or null (if unchanged).
     * The parent component saves the configuration (if provided) and navigates backward.
     */
    onBack(): void {
        const stepData = this.stepConfiguration?.();
        if (!this.stepForm.dirty || !stepData) {
            // No changes - emit with null configuration
            this.back.emit({ stepName: this.stepName(), configuration: null });
            return;
        }

        // Build changed configuration and emit
        const configuration = this.buildChangedConfiguration();
        this.back.emit({ stepName: this.stepName(), configuration });
    }

    /**
     * Get the current step's configuration data for saving.
     * Used by the parent wizard to retrieve form data before header-click navigation.
     */
    getConfigurationForSave(): {
        stepName: string;
        configuration: ConfigurationStepConfiguration | null;
        isDirty: boolean;
    } {
        const stepData = this.stepConfiguration?.();
        const isDirtyFlag = this.stepForm.dirty && !!stepData;
        return {
            stepName: this.stepName(),
            configuration: isDirtyFlag ? this.buildChangedConfiguration() : null,
            isDirty: isDirtyFlag
        };
    }

    /**
     * Build a configuration object containing only properties that have changed from their original values.
     * Uses the same value derivation logic as form initialization to ensure consistent comparison.
     * This method is used by both verify and save operations to ensure aligned behavior.
     */
    private buildChangedConfiguration(): ConfigurationStepConfiguration | null {
        const stepData = this.stepConfiguration?.();
        if (!stepData) return null;

        const formValues = this.stepForm.getRawValue();

        return {
            configurationStepName: stepData.configurationStepName,
            configurationStepDescription: stepData.configurationStepDescription,
            dependencies: stepData.dependencies,
            propertyGroupConfigurations: stepData.propertyGroupConfigurations
                .map((group) => {
                    const propertyValues = Object.keys(group.propertyDescriptors).reduce(
                        (values, propertyName) => {
                            const property = group.propertyDescriptors[propertyName];
                            const currentValue = formValues[property.name];

                            // Use the same fallback logic as form initialization for consistent comparison
                            // This ensures we compare against what the form was actually initialized with
                            const fallbackValue =
                                property.type === 'BOOLEAN' ? false : property.type === 'STRING_LIST' ? [] : '';
                            const apiValue = fromValueReference(group.propertyValues?.[property.name], property.type);

                            let originalValue = apiValue ?? property.defaultValue ?? fallbackValue;

                            // Normalize assets so comparison matches the form value shape
                            if (property.type === 'ASSET') {
                                // fromValueReference returns AssetReference|null; form stores string|null
                                originalValue = originalValue?.id ?? null;
                            } else if (property.type === 'ASSET_LIST') {
                                // fromValueReference returns AssetReference[]; form stores string[]
                                originalValue = Array.isArray(originalValue)
                                    ? originalValue.map((r: AssetReference) => r.id)
                                    : [];
                            }

                            // Only include fields that have changed
                            // For arrays (STRING_LIST), compare stringified versions
                            const hasChanged = Array.isArray(currentValue)
                                ? JSON.stringify(currentValue) !== JSON.stringify(originalValue)
                                : currentValue !== originalValue;

                            // Wrap primitive form values in ConnectorValueReference for API
                            // Pass property type to handle STRING_LIST array to comma-separated conversion
                            if (hasChanged) {
                                // For SECRET types, parse the composite key to get secret info
                                let secretOptions: SecretReferenceOptions | undefined;
                                if (property.type === 'SECRET' && currentValue) {
                                    // currentValue is a composite key: providerId::providerName::fullyQualifiedName
                                    const { providerId, fullyQualifiedName } = parseSecretKey(currentValue);
                                    const secrets = this.availableSecrets();
                                    // Find the matching secret using providerId and fullyQualifiedName
                                    const selectedSecret = secrets?.find(
                                        (s) =>
                                            s.providerId === providerId && s.fullyQualifiedName === fullyQualifiedName
                                    );
                                    if (selectedSecret) {
                                        secretOptions = {
                                            secretName: selectedSecret.name,
                                            providerId: selectedSecret.providerId,
                                            providerName: selectedSecret.providerName,
                                            fullyQualifiedSecretName: selectedSecret.fullyQualifiedName
                                        };
                                    }
                                }
                                values[property.name] = toValueReference(currentValue, property.type, secretOptions);
                            }
                            return values;
                        },
                        {} as Record<string, ConnectorValueReference>
                    );

                    return {
                        propertyGroupName: group.propertyGroupName,
                        propertyGroupDescription: group.propertyGroupDescription,
                        propertyDescriptors: group.propertyDescriptors,
                        propertyValues
                    };
                })
                // Only include groups that have changed values
                .filter((group) => Object.keys(group.propertyValues).length > 0)
        };
    }

    /**
     * Handle verify button click
     */
    onVerify(): void {
        const stepData = this.stepConfiguration?.();
        // If form is invalid, mark all fields as touched to show validation errors
        if (!this.stepForm.valid || !stepData) {
            this.markAllAsTouched();
            return;
        }

        const configurationToVerify = this.buildChangedConfiguration();
        if (!configurationToVerify) return;

        this.wizardStore.verifyStep({
            stepName: this.stepName(),
            configuration: configurationToVerify
        });
    }

    /**
     * Handle save & continue button click
     */
    onSave(): void {
        const stepData = this.stepConfiguration?.();
        // Mark all fields as touched to show any validation errors
        this.markAllAsTouched();

        // If form is not dirty, skip the save and just advance
        // (even if there are validation errors - backend is the final validator)
        if (!this.stepForm.dirty && stepData) {
            this.wizardStore.advanceWithoutSaving(this.stepName());
            return;
        }

        // If form is dirty, save the changed configuration
        // (even if there are validation errors - backend is the final validator)
        if (this.stepForm.dirty && stepData) {
            const updatedConfiguration = this.buildChangedConfiguration();
            if (!updatedConfiguration) return;

            this.wizardStore.saveStep({
                stepName: this.stepName(),
                stepConfiguration: updatedConfiguration
            });
        }
    }

    // ========================================================================================
    // Asset Upload Handling (uses UploadService directly instead of NgRx upload state)
    // ========================================================================================

    /**
     * Get the current assets for a property.
     * Used in the template to pass to connector-property-input.
     */
    getAssetsForProperty(propertyName: string): AssetInfo[] {
        return this.assetsByPropertySignal()[propertyName] || [];
    }

    /**
     * Get the upload progress for a property.
     * Used in the template to pass to connector-property-input.
     */
    getUploadProgressForProperty(propertyName: string): UploadProgressInfo[] {
        return this.uploadProgressByProperty.get(propertyName) || [];
    }

    /**
     * Handle file selection from the asset upload component.
     * Creates upload requests and manages progress tracking.
     */
    onAssetFilesSelected(propertyName: string, files: File[]): void {
        const connId = this.connectorId();
        if (!connId) return;

        // Check if this is a single asset property (ASSET vs ASSET_LIST)
        const property = this.findPropertyDescriptor(propertyName);
        const isSingleAsset = property?.type === 'ASSET';

        // For single asset type, clear existing assets before uploading new one
        // Also clear the form control value and mark dirty so save happens even if upload fails
        const control = this.stepForm.get(propertyName);
        if (isSingleAsset) {
            this.wizardStore.clearAssetsForProperty(propertyName);
            if (control) {
                control.setValue(null);
                control.markAsDirty();
            }
            this.uploadProgressByProperty.delete(propertyName);
        }

        // Clear error state while upload is in progress - user is actively providing a value
        if (control) {
            const assets = this.assetsByPropertySignal()[propertyName] ?? [];
            const anyMissingContent = assets.some((a) => a.missingContent);
            if (!anyMissingContent) {
                control.markAsUntouched();
            }
            control.updateValueAndValidity();
        }
        this.cdr.detectChanges();

        let nextUploadId = Date.now();
        for (const file of files) {
            const uploadId = nextUploadId++;
            const request = this.connectorConfigService.createAssetUploadRequest(connId, file, propertyName);

            const currentProgress = this.uploadProgressByProperty.get(propertyName) || [];
            currentProgress.push({
                filename: file.name,
                percentComplete: 0,
                status: 'active'
            });
            this.uploadProgressByProperty.set(propertyName, currentProgress);
            this.cdr.detectChanges();

            const sub = (this.uploadService.upload(request) as Observable<HttpEvent<unknown>>).subscribe({
                next: (event: HttpEvent<unknown>) => {
                    if (event.type === HttpEventType.UploadProgress && event.total) {
                        const percent = Math.round((100 * event.loaded) / event.total);
                        this.updateUploadProgress(propertyName, file.name, percent);
                    } else if (event.type === HttpEventType.Response) {
                        const body = event.body as { asset?: AssetInfo } | null;
                        const assetData = body?.asset;
                        if (assetData?.id) {
                            this.wizardStore.addAsset({
                                propertyName,
                                asset: {
                                    id: assetData.id,
                                    name: assetData.name || file.name,
                                    digest: assetData.digest,
                                    missingContent: assetData.missingContent
                                },
                                uploadId
                            });
                        }
                        this.removeUploadProgress(propertyName, file.name);
                    }
                },
                error: (err: HttpErrorResponse) => {
                    this.setUploadError(propertyName, file.name, err.message || 'Upload failed');
                    const ctrl = this.stepForm.get(propertyName);
                    if (ctrl) {
                        ctrl.markAsTouched();
                    }
                    this.cdr.detectChanges();
                }
            });

            this.activeUploadSubscriptions.push(sub);
        }
    }

    private updateUploadProgress(propertyName: string, filename: string, percent: number): void {
        const progress = this.uploadProgressByProperty.get(propertyName) || [];
        const entry = progress.find((p) => p.filename === filename);
        if (entry) {
            entry.percentComplete = percent;
        }
        this.cdr.detectChanges();
    }

    private removeUploadProgress(propertyName: string, filename: string): void {
        const progress = this.uploadProgressByProperty.get(propertyName) || [];
        const filtered = progress.filter((p) => p.filename !== filename);
        if (filtered.length > 0) {
            this.uploadProgressByProperty.set(propertyName, filtered);
        } else {
            this.uploadProgressByProperty.delete(propertyName);
        }
        this.cdr.detectChanges();
    }

    private setUploadError(propertyName: string, filename: string, error: string): void {
        const progress = this.uploadProgressByProperty.get(propertyName) || [];
        const entry = progress.find((p) => p.filename === filename);
        if (entry) {
            entry.status = 'error';
            entry.error = error;
            entry.percentComplete = 0;
        }
    }

    /**
     * Handle asset delete request from the asset upload component.
     * Removes from store state and updates form control.
     */
    onAssetDeleteRequested(propertyName: string, asset: AssetInfo): void {
        this.wizardStore.removeAsset({ propertyName, assetId: asset.id });

        // Update form control value
        const control = this.stepForm.get(propertyName);
        if (control) {
            const property = this.findPropertyDescriptor(propertyName);
            const currentAssets = this.assetsByPropertySignal()[propertyName] || [];
            const updatedAssets = currentAssets.filter((a) => a.id !== asset.id);

            if (property?.type === 'ASSET') {
                control.setValue(null);
            } else if (property?.type === 'ASSET_LIST') {
                control.setValue(updatedAssets.map((a) => a.id));
            }
            control.markAsDirty();
            control.markAsTouched();
        }

        this.cdr.detectChanges();
    }

    /**
     * Handle dismiss request for a failed upload.
     * Clears the failed upload progress entry.
     */
    onDismissFailedUpload(_propertyName: string, progress: UploadProgressInfo): void {
        const currentProgress = this.uploadProgressByProperty.get(_propertyName) || [];
        const filtered = currentProgress.filter((p) => p.filename !== progress.filename);
        if (filtered.length > 0) {
            this.uploadProgressByProperty.set(_propertyName, filtered);
        } else {
            this.uploadProgressByProperty.delete(_propertyName);
        }
        this.cdr.detectChanges();
    }

    /**
     * Build assets from existing asset references for initialization.
     * Returns the assets array to be collected and initialized in bulk.
     */
    private buildAssetsFromValue(
        propertyName: string,
        propertyType: string,
        value: AssetReference | AssetReference[] | null
    ): AssetInfo[] {
        if (!value) return [];

        const assets: AssetInfo[] = [];

        if (propertyType === 'ASSET') {
            // Single asset reference: { id, name }
            if (!Array.isArray(value) && typeof value === 'object' && value.id) {
                assets.push({
                    id: value.id,
                    name: value.name || value.id,
                    missingContent: value.missingContent
                });
            }
        } else if (propertyType === 'ASSET_LIST') {
            // Array of asset references
            if (Array.isArray(value)) {
                value.forEach((ref: AssetReference) => {
                    if (ref && typeof ref === 'object' && ref.id) {
                        assets.push({
                            id: ref.id,
                            name: ref.name || ref.id,
                            missingContent: ref.missingContent
                        });
                    }
                });
            }
        }

        return assets;
    }

    /**
     * Check if the current step has documentation available
     */
    hasDocumentation(): boolean {
        return this.stepConfiguration?.()?.documented === true;
    }

    onDismissGeneralVerificationErrors(): void {
        this.wizardStore.setStepVerificationResults({ stepName: this.stepName(), results: [] });
    }

    /**
     * Toggle the documentation panel
     */
    toggleDocumentationPanel(): void {
        const currentOpen = this.documentationPanelOpen();
        this.wizardStore.toggleDocumentationPanel(!currentOpen);
    }

    /**
     * Close the documentation panel
     */
    closeDocumentationPanel(): void {
        this.wizardStore.toggleDocumentationPanel(false);
    }

    ngOnDestroy(): void {
        // Unregister from the ActiveStepService to prevent stale references
        this.activeStepService?.unregister(this);

        for (const sub of this.activeUploadSubscriptions) {
            sub.unsubscribe();
        }
    }
}
