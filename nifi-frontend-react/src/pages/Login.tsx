import React from 'react';
import { Activity, Shield, UserCheck } from 'lucide-react';

const Login: React.FC = () => {
  return (
    <div className="min-h-screen bg-gradient-to-br from-primary-50 to-primary-100 flex items-center justify-center p-4">
      <div className="max-w-md w-full">
        <div className="bg-white rounded-lg shadow-lg p-8">
          <div className="text-center mb-8">
            <Activity className="h-12 w-12 text-primary-600 mx-auto mb-4" />
            <h1 className="text-2xl font-bold text-gray-900">Apache NiFi</h1>
            <p className="text-gray-600 mt-2">Data Flow Management System</p>
          </div>

          <div className="space-y-6">
            <div className="flex items-center p-4 bg-yellow-50 border border-yellow-200 rounded-md">
              <Shield className="h-5 w-5 text-yellow-600 mr-3 flex-shrink-0" />
              <div className="text-sm text-yellow-800">
                <p className="font-medium">Authentication Required</p>
                <p className="mt-1">Please authenticate to access the NiFi interface.</p>
              </div>
            </div>

            <div className="text-center">
              <UserCheck className="h-8 w-8 text-primary-600 mx-auto mb-3" />
              <p className="text-sm text-gray-600">
                This system uses external authentication. 
                Please contact your system administrator if you're having trouble logging in.
              </p>
            </div>

            <div className="border-t pt-6">
              <div className="text-xs text-gray-500 text-center">
                <p>Apache NiFi is a powerful, scalable, and secure data routing,</p>
                <p>transformation, and system mediation system.</p>
              </div>
            </div>
          </div>
        </div>

        <div className="mt-8 text-center">
          <p className="text-sm text-primary-600">
            Powered by <a href="https://nifi.apache.org" className="underline hover:text-primary-700">Apache NiFi</a>
          </p>
        </div>
      </div>
    </div>
  );
};

export default Login;
