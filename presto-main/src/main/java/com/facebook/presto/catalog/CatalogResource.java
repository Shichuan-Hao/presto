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

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * @author Steven Hao
 * @date 6/16/23 11:40 AM
 * @description
 */
@Path("/v1/catalog")
public class CatalogResource
{
    private static final Logger log = Logger.get(CatalogResource.class);
    private final DynamicCatalogStoreConfig config;

    @Inject
    public CatalogResource(DynamicCatalogStoreConfig config)
    {
        this.config = requireNonNull(config, "dynamicCatalogStoreConfig is null");
    }

    @GET
    @Path("test")
    public Response test()
    {
        return Response.ok("Hello Presto").build();
    }

    @PUT
    @Path("add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog(CatalogInfo catalogInfo)
    {
        log.info("\n\n create new catalog beginning.... \n\n");
        requireNonNull(catalogInfo, "catalogInfo is null");
        // 1. 先根据 catalog name 查询，catalog 是否存在
        String catalogName = catalogInfo.getCatalogName();
        if (!verify(catalogName)) {
            log.error("The catalog name only lowercase numbers and digits are allowed!");
            return Response.serverError()
                    .entity("The catalog name only lowercase numbers and digits are allowed!")
                    .build();
        }
        if (!checkCatalogIsExistByCatalogName(catalogName)) {
            log.error("The " + catalogName + " is exist");
            return Response.serverError()
                    .entity("The " + catalogName + " is exist")
                    .build();
        }
        if (insert(catalogInfo, ConnectionConfig.insertCatalogSql) > 0) {
            return Response.ok().build();
        }
        return Response.serverError().build();
    }

    private int insert(CatalogInfo catalogInfo, String sql)
    {
        JsonCodec<Map> codec = JsonCodec.jsonCodec(Map.class);
        int result = 0;
        try (Connection connection = ConnectionConfig.openConnection(config);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, catalogInfo.getCatalogName());
            preparedStatement.setString(2, catalogInfo.getConnectorName());
            preparedStatement.setString(3, catalogInfo.getCreator());
            preparedStatement.setString(4, codec.toJson(catalogInfo.getProperties()));
            result = preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            log.error("insert error, the error is {}", e.getMessage());
        }
        return result;
    }

    private boolean checkCatalogIsExistByCatalogName(String catalogName)
    {
        JsonCodec<Map> codec = JsonCodec.jsonCodec(Map.class);
        try (Connection connection = ConnectionConfig.openConnection(config);
                PreparedStatement preparedStatement = buildCheckCatalogIsExistByCatalogNamePreparedStatement(connection, catalogName);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                log.info("The number of results of the query by [%s] is [%s]", catalogName, count);
                return count <= 0;
            }
        }
        catch (SQLException e) {
            log.error("Check if an exception exists for [catalog] by [%s], the exception message is [%s]",
                    catalogName, e.getMessage());
            return false;
        }
        return true;
    }

    private PreparedStatement buildCheckCatalogIsExistByCatalogNamePreparedStatement(
            Connection connection,
            String catalogName)
            throws SQLException
    {
        String checkCatalogExistsSqlByCatalogName = ConnectionConfig.checkCatalogExistsSqlByCatalogName;
        log.info("check catalog exists sql by catalog name: {}", checkCatalogExistsSqlByCatalogName);
        PreparedStatement preparedStatement = connection
                .prepareStatement(checkCatalogExistsSqlByCatalogName);
        preparedStatement.setString(1, catalogName);
        return preparedStatement;
    }

    @DELETE
    @Path("delete")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteCatalog(DeleteCatalogQuery deleteCatalog)
    {
        log.info("\n\n delete catalog beginning.... \n\n");
        requireNonNull(deleteCatalog, "catalogName is null");
        try (Connection connection = ConnectionConfig.openConnection(config);
                PreparedStatement preparedStatement = connection.prepareStatement(ConnectionConfig.deleteCatalogSqlByCatalogName)) {
            preparedStatement.setString(1, deleteCatalog.getCatalogName());
//            preparedStatement.setString(2, catalogInfo.getConnectorName());
//            preparedStatement.setString(3, catalogInfo.getCreator());
            preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.status(Response.Status.OK).build();
    }

    private Boolean verify(String str)
    {
        final String pattern = "^[a-z0-9]+$";
        Pattern compile = Pattern.compile(pattern);
        Matcher matcher = compile.matcher(str);
        return matcher.matches();
    }
}
