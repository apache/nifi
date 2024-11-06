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

import { routerReducer, RouterReducerState, DEFAULT_ROUTER_FEATURENAME } from '@ngrx/router-store';
import { ActionReducerMap } from '@ngrx/store';
import { CurrentUserState, currentUserFeatureKey } from './current-user';
import { currentUserReducer } from './current-user/current-user.reducer';
import { extensionTypesFeatureKey, ExtensionTypesState } from './extension-types';
import { extensionTypesReducer } from './extension-types/extension-types.reducer';
import { aboutFeatureKey, AboutState } from './about';
import { aboutReducer } from './about/about.reducer';
import { statusHistoryFeatureKey, StatusHistoryState } from './status-history';
import { statusHistoryReducer } from './status-history/status-history.reducer';
import { controllerServiceStateFeatureKey, ControllerServiceState } from './contoller-service-state';
import { controllerServiceStateReducer } from './contoller-service-state/controller-service-state.reducer';
import { systemDiagnosticsFeatureKey, SystemDiagnosticsState } from './system-diagnostics';
import { systemDiagnosticsReducer } from './system-diagnostics/system-diagnostics.reducer';
import { flowConfigurationFeatureKey, FlowConfigurationState } from './flow-configuration';
import { flowConfigurationReducer } from './flow-configuration/flow-configuration.reducer';
import { componentStateFeatureKey, ComponentStateState } from './component-state';
import { componentStateReducer } from './component-state/component-state.reducer';
import { errorFeatureKey, ErrorState } from './error';
import { errorReducer } from './error/error.reducer';
import { clusterSummaryFeatureKey, ClusterSummaryState } from './cluster-summary';
import { clusterSummaryReducer } from './cluster-summary/cluster-summary.reducer';
import { loginConfigurationFeatureKey, LoginConfigurationState } from './login-configuration';
import { loginConfigurationReducer } from './login-configuration/login-configuration.reducer';
import { propertyVerificationFeatureKey, PropertyVerificationState } from './property-verification';
import { propertyVerificationReducer } from './property-verification/property-verification.reducer';
import { navigationFeatureKey, NavigationState } from './navigation';
import { navigationReducer } from './navigation/navigation.reducer';
import { bannerTextFeatureKey, BannerTextState } from './banner-text';
import { bannerTextReducer } from './banner-text/banner-text.reducer';
import { documentVisibilityFeatureKey, DocumentVisibilityState } from './document-visibility';
import { documentVisibilityReducer } from './document-visibility/document-visibility.reducer';
import { copyFeatureKey, CopyState } from './copy';
import { copyReducer } from './copy/copy.reducer';

export interface NiFiState {
    [DEFAULT_ROUTER_FEATURENAME]: RouterReducerState;
    [errorFeatureKey]: ErrorState;
    [currentUserFeatureKey]: CurrentUserState;
    [extensionTypesFeatureKey]: ExtensionTypesState;
    [aboutFeatureKey]: AboutState;
    [bannerTextFeatureKey]: BannerTextState;
    [navigationFeatureKey]: NavigationState;
    [flowConfigurationFeatureKey]: FlowConfigurationState;
    [loginConfigurationFeatureKey]: LoginConfigurationState;
    [statusHistoryFeatureKey]: StatusHistoryState;
    [controllerServiceStateFeatureKey]: ControllerServiceState;
    [systemDiagnosticsFeatureKey]: SystemDiagnosticsState;
    [componentStateFeatureKey]: ComponentStateState;
    [documentVisibilityFeatureKey]: DocumentVisibilityState;
    [clusterSummaryFeatureKey]: ClusterSummaryState;
    [propertyVerificationFeatureKey]: PropertyVerificationState;
    [copyFeatureKey]: CopyState;
}

export const rootReducers: ActionReducerMap<NiFiState> = {
    [DEFAULT_ROUTER_FEATURENAME]: routerReducer,
    [errorFeatureKey]: errorReducer,
    [currentUserFeatureKey]: currentUserReducer,
    [extensionTypesFeatureKey]: extensionTypesReducer,
    [aboutFeatureKey]: aboutReducer,
    [bannerTextFeatureKey]: bannerTextReducer,
    [navigationFeatureKey]: navigationReducer,
    [flowConfigurationFeatureKey]: flowConfigurationReducer,
    [loginConfigurationFeatureKey]: loginConfigurationReducer,
    [statusHistoryFeatureKey]: statusHistoryReducer,
    [controllerServiceStateFeatureKey]: controllerServiceStateReducer,
    [systemDiagnosticsFeatureKey]: systemDiagnosticsReducer,
    [componentStateFeatureKey]: componentStateReducer,
    [documentVisibilityFeatureKey]: documentVisibilityReducer,
    [clusterSummaryFeatureKey]: clusterSummaryReducer,
    [propertyVerificationFeatureKey]: propertyVerificationReducer,
    [copyFeatureKey]: copyReducer
};
