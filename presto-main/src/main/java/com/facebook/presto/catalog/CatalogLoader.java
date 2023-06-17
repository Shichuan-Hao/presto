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
package com.facebook.presto.catalog;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;

/**
 * @author Steven Hao
 * @date 6/15/23 9:09 AM
 * @description
 */
public class CatalogLoader
{
    private static final Logger log = Logger.get(CatalogLoader.class);
    protected final DynamicCatalogStoreConfig config;
    public CatalogLoader(DynamicCatalogStoreConfig config)
    {
        this.config = config;
    }

    public ImmutableMap<String, CatalogInfo> load() throws Exception
    {
        ImmutableMap.Builder<String, CatalogInfo> builder = ImmutableMap.builder();
        try (Connection connection = ConnectionConfig.openConnection(config);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(ConnectionConfig.getCatalogSql)) {
            JsonCodec<Map> mapJsonCodec = jsonCodec(Map.class);
            JsonCodec<CatalogInfo> catalogJsonCodec = jsonCodec(CatalogInfo.class);
            while (resultSet.next()) {
                String catalogName = resultSet.getString("catalog_name");
                String connectorName = resultSet.getString("connector_name");
                String creator = resultSet.getString("creator");
                String properties = resultSet.getString("properties");
                if (!Strings.isNullOrEmpty(catalogName)
                        && !Strings.isNullOrEmpty(connectorName)
                        && !Strings.isNullOrEmpty(properties)) {
                    CatalogInfo catalogInfo = new CatalogInfo(catalogName,
                            connectorName, creator, mapJsonCodec.fromJson(properties));
                    String md5 = Hashing.md5().hashBytes(catalogJsonCodec.toJsonBytes(catalogInfo))
                            .toString();
                    builder.put(md5, catalogInfo);
                }
            }
        }
        return builder.build();
    }
}
