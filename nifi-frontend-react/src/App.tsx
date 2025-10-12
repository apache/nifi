
import React from 'react';
import { Provider } from 'react-redux';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { store } from './state/store';
import FlowDesigner from './pages/FlowDesigner';
import AuthenticationGuard from './components/guards/AuthenticationGuard';
import LoginConfigurationGuard from './components/guards/LoginConfigurationGuard';
import Login from './pages/Login';
import Layout from './components/Layout';

function App(): JSX.Element {
  return (
    <Provider store={store}>
      <BrowserRouter>
        <Routes>
          <Route
            path="/login"
            element={
              <LoginConfigurationGuard
                check={(loginConfiguration) => loginConfiguration?.loginSupported === true}
                errorFallback={(message: string) => (
                  <div className="flex items-center justify-center min-h-screen">
                    <div className="text-center">
                      <h2 className="text-xl font-semibold text-red-600 mb-2">Configuration Error</h2>
                      <p className="text-gray-600">{message}</p>
                    </div>
                  </div>
                )}
              >
                <Login />
              </LoginConfigurationGuard>
            }
          />
          <Route
            path="/nifi/*"
            element={
              <AuthenticationGuard>
                <Layout>
                  <Routes>
                    <Route path="flow-designer/*" element={<FlowDesigner />} />
                    <Route path="*" element={<Navigate to="/nifi/flow-designer" replace />} />
                  </Routes>
                </Layout>
              </AuthenticationGuard>
            }
          />
          <Route path="/" element={<Navigate to="/nifi/flow-designer" replace />} />
          <Route path="*" element={<Navigate to="/nifi/flow-designer" replace />} />
        </Routes>
      </BrowserRouter>
    </Provider>
  );
}

export default App;
