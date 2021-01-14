/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kusto;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class KustoConfig
{
    // https://azure.microsoft.com/en-in/services/data-explorer/
    // clusterPath is typically like: https://<cluster name>.<location>.kusto.windows.net/
    private String clusterPath;
    // Azure data explorer supports AAD authentication. A new AAD app needs to be created
    // to access the kusto cluster. Steps for configuration and to get the tenantID, appID
    // and secret are listed in this link:
    // https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app
    private String appTenantId;
    private String appId;
    private String appSecret;

    @NotNull
    public String getClusterPath()
    {
        return clusterPath;
    }

    @Config("cluster-path")
    public KustoConfig setClusterPath(String path)
    {
        this.clusterPath = path;
        return this;
    }

    @NotNull
    public String getAppTenantId()
    {
        return appTenantId;
    }

    @Config("app-tenant-id")
    public KustoConfig setAppTenantId(String tenantId)
    {
        this.appTenantId = tenantId;
        return this;
    }

    @NotNull
    public String getAppId()
    {
        return appId;
    }

    @Config("app-id")
    public KustoConfig setAppId(String clientId)
    {
        this.appId = clientId;
        return this;
    }

    @NotNull
    public String getAppSecret()
    {
        return appSecret;
    }

    @Config("app-secret")
    public KustoConfig setAppSecret(String secret)
    {
        this.appSecret = secret;
        return this;
    }
}
