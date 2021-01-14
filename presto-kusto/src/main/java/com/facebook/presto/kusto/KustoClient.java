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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableSet;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import javax.inject.Inject;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class KustoClient
{
    private final ClientImpl remoteClient;
    private static final Logger log = Logger.get(KustoClient.class);
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private Map<String, Map<String, KustoTable>> schemas;

    @Inject
    public KustoClient(KustoConfig config, JsonCodec<Map<String, List<KustoTable>>> catalogCodec) throws URISyntaxException
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        log.info("Initializing kusto client");

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                config.getClusterPath(),
                config.getAppId(),
                config.getAppSecret(),
                config.getAppTenantId());

        remoteClient = new ClientImpl(csb);

        updateSchemasAndTables();
    }

    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, KustoTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public KustoTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, KustoTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    public KustoResultSetTable runQuery(String database, String command) throws DataClientException, DataServiceException
    {
        KustoOperationResult results = remoteClient.execute(database, command);
        return results.getPrimaryResults();
    }

    private Type getPrestoTypeFromKustoType(String kustoType) throws Exception
    {
        switch (kustoType) {
            case "System.Boolean":
                return BOOLEAN;
            case "System.DateTime":
                return TIMESTAMP;
            case "System.Object":
                return JSON;
            case "System.Guid":
            case "System.String":
                return VARCHAR;
            case "System.Int32":
                return INTEGER;
            case "System.Int64":
                return BIGINT;
            case "System.Double":
            case "System.Data.SqlTypes.SqlDecimal":
                return DOUBLE;
            default:
                throw new Exception("Unsupported type");
        }
    }

    private void updateSchemasAndTables()
    {
        try {
            KustoResultSetTable mainTableResult = runQuery("master", ".show databases schema");

            // iterate values
            HashMap<String, HashSet<String>> schemaToTablesMap = new HashMap<>();
            HashMap<String, ArrayList<KustoColumn>> tableToColumnMap = new HashMap<>();
            while (mainTableResult.next()) {
                ArrayList<Object> row = mainTableResult.getCurrentRow();
                String dbName;
                if (row.get(0) != null) {
                    dbName = row.get(0).toString();
                }
                else {
                    continue;
                }
                HashSet<String> tableSet = schemaToTablesMap.get(dbName);
                if (tableSet == null) {
                    tableSet = new HashSet();
                    schemaToTablesMap.put(dbName, tableSet);
                }
                String tableName = null;
                if (row.get(1) != null) {
                    tableName = row.get(1).toString();
                    tableSet.add(tableName);
                }
                else {
                    continue;
                }
                if (tableName == null) {
                    continue;
                }
                ArrayList<KustoColumn> tableColumns = tableToColumnMap.get(dbName + tableName);
                if (tableColumns == null) {
                    tableColumns = new ArrayList<>();
                    tableToColumnMap.put(dbName + tableName, tableColumns);
                }
                String columnName;
                if (row.get(2) != null) {
                    columnName = row.get(2).toString();
                }
                else {
                    continue;
                }
                String columnType = row.get(3).toString();
                tableColumns.add(new KustoColumn(columnName, getPrestoTypeFromKustoType(columnType)));
            }
            for (Map.Entry<String, HashSet<String>> entry : schemaToTablesMap.entrySet()) {
                HashMap<String, KustoTable> allTables = new HashMap<>();
                if (schemas == null) {
                    schemas = new HashMap<>();
                }
                schemas.put(entry.getKey(), allTables);
                for (String tableName : entry.getValue()) {
                    KustoTable table = new KustoTable(tableName, tableToColumnMap.get(entry.getKey() + tableName));
                    allTables.put(tableName, table);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
