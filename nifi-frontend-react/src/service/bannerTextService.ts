// BannerTextService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface BannerTextEntity {
  // Define properties based on expected API response
  banners?: any;
}

export function getBannerText$(): Observable<BannerTextEntity> {
  return ajax.getJSON<BannerTextEntity>(`${API}/flow/banners`).pipe(
    map(response => response)
  );
}
