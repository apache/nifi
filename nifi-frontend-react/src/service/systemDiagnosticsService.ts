// SystemDiagnosticsService migrated from Angular with TypeScript and RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export function getSystemDiagnostics$(nodewise = false): Observable<unknown> {
  const baseUrl = `${API}/system-diagnostics`;
  const url = nodewise ? `${baseUrl}?nodewise=true` : baseUrl;
  return ajax.getJSON(url).pipe(map((response) => response));
}
