// ExtensionTypesService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface ProcessorTypesResponse {
  // Define properties based on expected API response
  types?: any;
}

export interface ProcessorVersionsResponse {
  // Define properties based on expected API response
  versions?: any;
}

export interface BundleDescriptor {
  group: string;
  artifact: string;
  version?: string;
}

export interface ControllerServiceTypesResponse {
  controllerServiceTypes?: any;
}

export interface ReportingTaskTypesResponse {
  reportingTaskTypes?: any;
}

export interface RegistryClientTypesResponse {
  registryClients?: any;
}

export interface PrioritizersResponse {
  prioritizers?: any;
}

export interface FlowAnalysisRuleTypesResponse {
  flowAnalysisRuleTypes?: any;
}

export interface ParameterProviderTypesResponse {
  parameterProviderTypes?: any;
}

export function getProcessorTypes$(): Observable<ProcessorTypesResponse> {
  return ajax.getJSON<ProcessorTypesResponse>(`${API}/flow/processor-types`).pipe(
    map(response => response)
  );
}

export function getProcessorVersionsForType$(processorType: string, bundle: { group: string; artifact: string }): Observable<ProcessorVersionsResponse> {
  const params = new URLSearchParams({
    bundleGroupFilter: bundle.group,
    bundleArtifactFilter: bundle.artifact,
    type: processorType
  });
  return ajax.getJSON<ProcessorVersionsResponse>(`${API}/flow/processor-types?${params}`).pipe(
    map(response => response)
  );
}

export function getControllerServiceTypes$(): Observable<ControllerServiceTypesResponse> {
  return ajax
    .getJSON<ControllerServiceTypesResponse>(`${API}/flow/controller-service-types`)
    .pipe(map((response) => response));
}

export function getControllerServiceVersionsForType$(
  serviceType: string,
  bundle: BundleDescriptor
): Observable<ControllerServiceTypesResponse> {
  const params = new URLSearchParams();
  params.set('serviceBundleGroup', bundle.group);
  params.set('serviceBundleArtifact', bundle.artifact);
  params.set('typeFilter', serviceType);
  return ajax
    .getJSON<ControllerServiceTypesResponse>(`${API}/flow/controller-service-types?${params.toString()}`)
    .pipe(map((response) => response));
}

export function getImplementingControllerServiceTypes$(
  serviceType: string,
  bundle: BundleDescriptor
): Observable<ControllerServiceTypesResponse> {
  const params = new URLSearchParams();
  params.set('serviceType', serviceType);
  params.set('serviceBundleGroup', bundle.group);
  params.set('serviceBundleArtifact', bundle.artifact);
  if (bundle.version) {
    params.set('serviceBundleVersion', bundle.version);
  }
  return ajax
    .getJSON<ControllerServiceTypesResponse>(`${API}/flow/controller-service-types?${params.toString()}`)
    .pipe(map((response) => response));
}

export function getReportingTaskTypes$(): Observable<ReportingTaskTypesResponse> {
  return ajax
    .getJSON<ReportingTaskTypesResponse>(`${API}/flow/reporting-task-types`)
    .pipe(map((response) => response));
}

export function getReportingTaskVersionsForType$(
  reportingTaskType: string,
  bundle: BundleDescriptor
): Observable<ReportingTaskTypesResponse> {
  const params = new URLSearchParams();
  params.set('serviceBundleGroup', bundle.group);
  params.set('serviceBundleArtifact', bundle.artifact);
  params.set('type', reportingTaskType);
  return ajax
    .getJSON<ReportingTaskTypesResponse>(`${API}/flow/reporting-task-types?${params.toString()}`)
    .pipe(map((response) => response));
}

export function getRegistryClientTypes$(): Observable<RegistryClientTypesResponse> {
  return ajax
    .getJSON<RegistryClientTypesResponse>(`${API}/controller/registry-types`)
    .pipe(map((response) => response));
}

export function getPrioritizers$(): Observable<PrioritizersResponse> {
  return ajax
    .getJSON<PrioritizersResponse>(`${API}/flow/prioritizers`)
    .pipe(map((response) => response));
}

export function getFlowAnalysisRuleTypes$(): Observable<FlowAnalysisRuleTypesResponse> {
  return ajax
    .getJSON<FlowAnalysisRuleTypesResponse>(`${API}/flow/flow-analysis-rule-types`)
    .pipe(map((response) => response));
}

export function getFlowAnalysisRuleVersionsForType$(
  flowAnalysisRuleType: string,
  bundle: BundleDescriptor
): Observable<FlowAnalysisRuleTypesResponse> {
  const params = new URLSearchParams();
  params.set('serviceBundleGroup', bundle.group);
  params.set('serviceBundleArtifact', bundle.artifact);
  params.set('type', flowAnalysisRuleType);
  return ajax
    .getJSON<FlowAnalysisRuleTypesResponse>(`${API}/flow/flow-analysis-rule-types?${params.toString()}`)
    .pipe(map((response) => response));
}

export function getParameterProviderTypes$(): Observable<ParameterProviderTypesResponse> {
  return ajax
    .getJSON<ParameterProviderTypesResponse>(`${API}/flow/parameter-provider-types`)
    .pipe(map((response) => response));
}
