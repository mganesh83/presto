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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KustoRecordSet
        implements RecordSet
{
    private final KustoSplit kustoSplit;
    private final List<KustoColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final KustoClient kustoClient;

    public KustoRecordSet(List<KustoColumnHandle> columnHandles,
                          KustoSplit split,
                          KustoClient client)
    {
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        this.kustoSplit = requireNonNull(split, "split is null");
        this.kustoClient = requireNonNull(client, "kusto client is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KustoColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        try {
            return new KustoRecordCursor(columnHandles, kustoSplit, kustoClient);
        }
        catch (Exception e) {
            return null;
        }
    }
}
