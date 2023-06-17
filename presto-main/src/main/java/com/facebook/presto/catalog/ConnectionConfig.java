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

import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Steven Hao
 * @date 6/16/23 11:22 AM
 * @description Connection Config
 */
public final class ConnectionConfig
{
    private ConnectionConfig()
    {
    }

    public static String getCatalogSql = "SELECT catalog_name, connector_name, creator, properties FROM catalog";

    public static String checkCatalogExistsSqlByCatalogName = "SELECT COUNT(*) FROM catalog WHERE catalog_name=?";

    public static String insertCatalogSql = "insert into catalog "
            + "(catalog_name, connector_name, creator, properties) values (?, ?, ?, ?)";

    public static String deleteCatalogSql = "delete from catalog where "
            + "catalog_name=? and connector_name=? and creator=?";
    public static String deleteCatalogSqlByCatalogName = "delete from catalog where "
            + "catalog_name=?";

    public static Connection openConnection(DynamicCatalogStoreConfig config)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        connectionProperties.setProperty("useSSL", "false");

        return new Driver().connect(config.getCatalogSourceMysqlUrl(), connectionProperties);
    }

    private static Properties basicConnectionProperties(DynamicCatalogStoreConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getCatalogSourceMysqlUser() != null) {
            connectionProperties.setProperty("user", config.getCatalogSourceMysqlUser());
        }
        if (config.getCatalogSourceMysqlPassword() != null) {
            connectionProperties.setProperty("password", config.getCatalogSourceMysqlPassword());
        }
        return connectionProperties;
    }
}
