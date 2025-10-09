// SystemDiagnosticsService migrated from Angular with TypeScript and RxJS
import { ajax } from 'rxjs/ajax';
import { Observable } from 'rxjs';

import type { SystemDiagnostics } from '../state/systemDiagnosticsSlice';

export const API = '../nifi-api';

export interface SystemDiagnosticsResponse {
  systemDiagnostics: SystemDiagnostics;
  [key: string]: unknown;
}

export function getSystemDiagnostics$(nodewise = false): Observable<SystemDiagnosticsResponse> {
  const baseUrl = `${API}/system-diagnostics`;
  const url = nodewise ? `${baseUrl}?nodewise=true` : baseUrl;
  return ajax.getJSON<SystemDiagnosticsResponse>(url);
}
