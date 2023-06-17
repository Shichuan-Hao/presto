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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.LegacyConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;

/**
 * @author Steven Hao
 * @date 6/15/23 9:08 AM
 * @description Dynamic Catalog Store Config
 */
public class DynamicCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private List<String> disabledCatalogs;
    private File catalogConfigurationDir = new File("etc/catalog/");
    private int catalogDetectTimeInterval = 10;
    private String catalogSourceMysqlUrl;
    private String catalogSourceMysqlUser;
    private String catalogSourceMysqlPassword;

    public File getCatalogConfigurationDir()
    {
        return catalogConfigurationDir;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-dir")
    public DynamicCatalogStoreConfig setCatalogConfigurationDir(File dir)
    {
        this.catalogConfigurationDir = dir;
        return this;
    }

    public List<String> getDisabledCatalogs()
    {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public DynamicCatalogStoreConfig setDisabledCatalogs(String catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : SPLITTER.splitToList(catalogs);
        return this;
    }

    public DynamicCatalogStoreConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }

    @Config("catalog.detect.time.interval")
    public DynamicCatalogStoreConfig setCatalogDetectTimeInterval(int interval)
    {
        this.catalogDetectTimeInterval = interval;
        return this;
    }

    public int getCatalogDetectTimeInterval()
    {
        return this.catalogDetectTimeInterval;
    }

    @Config("catalog.source.mysql.url")
    public DynamicCatalogStoreConfig setCatalogSourceMysqlUrl(String url)
    {
        this.catalogSourceMysqlUrl = url;
        return this;
    }

    public String getCatalogSourceMysqlUrl()
    {
        return this.catalogSourceMysqlUrl;
    }

    @Config("catalog.source.mysql.user")
    public DynamicCatalogStoreConfig setCatalogSourceMysqlUser(String user)
    {
        this.catalogSourceMysqlUser = user;
        return this;
    }

    public String getCatalogSourceMysqlUser()
    {
        return this.catalogSourceMysqlUser;
    }

    @Config("catalog.source.mysql.password")
    public DynamicCatalogStoreConfig setCatalogSourceMysqlPassword(String password)
    {
        this.catalogSourceMysqlPassword = password;
        return this;
    }

    public String getCatalogSourceMysqlPassword()
    {
        return this.catalogSourceMysqlPassword;
    }
}
