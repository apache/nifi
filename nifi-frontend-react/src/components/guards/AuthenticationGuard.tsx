import { ReactNode, useEffect, useMemo, useState } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { firstValueFrom } from 'rxjs';
import { AjaxError } from 'rxjs/ajax';

import { getLoginConfiguration$ } from '../../service/authService';
import { getUser$ } from '../../service/currentUserService';
import {
  setLoginConfiguration,
  setLoginConfigurationError,
  setLoginConfigurationStatus
} from '../../state/loginConfigurationSlice';
import {
  setCurrentUser,
  setCurrentUserError,
  setCurrentUserStatus
} from '../../state/currentUserSlice';
import { useAppDispatch, useAppSelector } from '../../hooks/redux';
import type { RootState } from '../../state/store';
import { ErrorResponse, getErrorString } from '../../service/errorHelper';

export interface AuthenticationGuardProps {
  children: ReactNode;
  loadingFallback?: ReactNode;
  errorFallback?: (message: string) => ReactNode;
  redirectPath?: string;
}

const defaultErrorMessage =
  'Unable to load authentication configuration. Please contact your system administrator.';

function toErrorResponse(error: unknown): ErrorResponse {
  if (typeof error === 'object' && error !== null && 'status' in error) {
    const ajaxError = error as AjaxError;
    return {
      status: ajaxError.status ?? 0,
      message: ajaxError.message,
      error: ajaxError.response
    };
  }

  return {
    status: 0,
    message: error instanceof Error ? error.message : undefined
  };
}

export const AuthenticationGuard = ({
  children,
  loadingFallback = <div>Loading…</div>,
  errorFallback,
  redirectPath = '/login'
}: AuthenticationGuardProps) => {
  const dispatch = useAppDispatch();
  const location = useLocation();
  const loginState = useAppSelector((state: RootState) => state.loginConfiguration);
  const currentUserState = useAppSelector((state: RootState) => state.currentUser);

  const [localError, setLocalError] = useState<string | null>(null);
  const [redirect, setRedirect] = useState(false);

  useEffect(() => {
    let isMounted = true;

    const loadAuthConfig = async () => {
      if (loginState.status !== 'idle') {
        return;
      }

      dispatch(setLoginConfigurationStatus('loading'));
      try {
        const response = await firstValueFrom(getLoginConfiguration$());
        if (!isMounted) return;
        dispatch(setLoginConfiguration(response.authenticationConfiguration));
        dispatch(setLoginConfigurationStatus('success'));
      } catch (error) {
        if (!isMounted) return;
        const errorResponse = toErrorResponse(error);
        const message =
          getErrorString(errorResponse) ?? defaultErrorMessage;
        dispatch(setLoginConfigurationError(message));
        setLocalError(message);
      }
    };

    loadAuthConfig();

    return () => {
      isMounted = false;
    };
  }, [dispatch, loginState.status]);

  useEffect(() => {
    let isMounted = true;

    const loadCurrentUser = async () => {
      if (loginState.status !== 'success') {
        return;
      }

      if (currentUserState.status === 'success' || currentUserState.status === 'loading') {
        return;
      }

      dispatch(setCurrentUserStatus('loading'));
      try {
        const user = await firstValueFrom(getUser$());
        if (!isMounted) return;
        dispatch(setCurrentUser(user));
        dispatch(setCurrentUserStatus('success'));
      } catch (error) {
        if (!isMounted) return;
        const errorResponse = toErrorResponse(error);
        const message = getErrorString(errorResponse);

        dispatch(setCurrentUserError(message));

        if (errorResponse.status === 401) {
          if (loginState.loginConfiguration?.loginSupported) {
            setRedirect(true);
          } else {
            setLocalError(message ?? 'Unauthorized');
          }
        } else {
          setLocalError(message ?? 'An unexpected error has occurred.');
        }
      }
    };

    loadCurrentUser();

    return () => {
      isMounted = false;
    };
  }, [dispatch, loginState.loginConfiguration, loginState.status, currentUserState.status]);

  useEffect(() => {
    if (loginState.status === 'success' && currentUserState.status === 'success') {
      setLocalError(null);
    }
  }, [loginState.status, currentUserState.status]);

  const isLoading = useMemo(() => {
    return (
      loginState.status === 'idle' ||
      loginState.status === 'loading' ||
      currentUserState.status === 'idle' ||
      currentUserState.status === 'loading'
    );
  }, [loginState.status, currentUserState.status]);

  if (redirect) {
    return <Navigate to={redirectPath} state={{ from: location }} replace />;
  }

  if (localError) {
    return errorFallback ? errorFallback(localError) : <div>{localError}</div>;
  }

  if (isLoading) {
    return <>{loadingFallback}</>;
  }

  return <>{children}</>;
};

export default AuthenticationGuard;
