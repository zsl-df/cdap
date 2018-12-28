/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.featureengineer.service.handler;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.enums.CorrelationMatrixSchema;
import co.cask.cdap.common.enums.VarianceInflationFactorSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.response.pojo.PipelineInfo;
import co.cask.cdap.featureengineer.utils.JSONInputParser;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

import com.google.gson.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public class BaseServiceHandler extends AbstractHttpServiceHandler {
    
    private static final Logger LOG = LoggerFactory.getLogger(BaseServiceHandler.class);
    
    /**
     * Sends the error response back to client.
     *
     * @param responder
     *            to respond to the service request.
     * @param message
     *            to be included as part of the error
     */
    protected final void error(HttpServiceResponder responder, String message) {
        JsonObject error = new JsonObject();
        error.addProperty("status", HttpURLConnection.HTTP_INTERNAL_ERROR);
        error.addProperty("message", message);
        sendJson(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, error.toString());
    }
    
    /**
     * Sends the error response back to client with error status.
     *
     * @param responder
     *            to respond to the service request.
     * @param message
     *            to be included as part of the error
     */
    protected final void error(HttpServiceResponder responder, int status, String message) {
        JsonObject error = new JsonObject();
        error.addProperty("status", status);
        error.addProperty("message", message);
        sendJson(responder, HttpURLConnection.HTTP_NOT_FOUND, error.toString());
    }
    
    /**
     * Returns a Json response back to client.
     *
     * @param responder
     *            to respond to the service request.
     * @param status
     *            code to be returned to client.
     * @param body
     *            to be sent back to client.
     */
    protected final void sendJson(HttpServiceResponder responder, int status, String body) {
        responder.send(status, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)), "application/json",
                new HashMap<String, String>());
    }
    
    /**
     * Sends the success response back to client.
     *
     * @param responder
     *            to respond to the service request.
     * @param message
     *            to be included as part of the error
     */
    protected final void success(HttpServiceResponder responder, String message) {
        JsonObject error = new JsonObject();
        error.addProperty("status", HttpURLConnection.HTTP_OK);
        error.addProperty("message", message);
        sendJson(responder, HttpURLConnection.HTTP_OK, error.toString());
    }
    
    public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
    }
    
    protected String readCDAPKeyValueTable(final KeyValueTable table, final String key) {
        byte[] value = table.read(key);
        return new String(value, StandardCharsets.UTF_8);
    }
    
    protected String[] getHostAndPort(HttpServiceRequest request) {
        return request.getHeader("Host").split(":");
    }
    
    protected ClientConfig getClientConfig(String[] hostAndPort) {
        return ClientConfig.builder()
                .setConnectionConfig(new ConnectionConfig(hostAndPort[0], Integer.parseInt(hostAndPort[1]), false))
                .build();
    }
    
    protected ClientConfig getClientConfig(HttpServiceRequest request) {
        String[] hostAndPort = request.getHeader("Host").split(":");
        return ClientConfig.builder()
                .setConnectionConfig(new ConnectionConfig(hostAndPort[0], Integer.parseInt(hostAndPort[1]), false))
                .build();
    }
    
    protected void deletePipeline(HttpServiceRequest request, String pipelineName) throws Exception {
        ClientConfig clientConfig = getClientConfig(request);
        ApplicationClient applicationClient = new ApplicationClient(clientConfig);
        ApplicationId appId = new ApplicationId("default", pipelineName);
        try {
            applicationClient.delete(appId);
            deleteAllDatasets(pipelineName, clientConfig);
        } catch (ApplicationNotFoundException | UnauthorizedException | IOException | UnauthenticatedException e) {
            LOG.error("Unable to delete pipeline = " + pipelineName, e);
            throw e;
        }
    }
    
    protected void deleteAllDatasets(String pipelineName, ClientConfig clientConfig) throws Exception {
        DatasetClient dataSetClient = new DatasetClient(clientConfig);
        deleteDataSet(pipelineName, dataSetClient);
        deleteDataSet(CorrelationMatrixSchema.getDataSetName(pipelineName), dataSetClient);
        deleteDataSet(VarianceInflationFactorSchema.getDataSetName(pipelineName), dataSetClient);
    }
    
    protected void deleteDataSet(String dataSetName, DatasetClient dataSetClient) throws Exception {
        DatasetId instance = new DatasetId("default", dataSetName);
        try {
            if (dataSetClient.exists(instance)) {
                dataSetClient.delete(instance);
            }
        } catch (DatasetNotFoundException | UnauthorizedException | IOException | UnauthenticatedException e) {
            LOG.error("Unable to delete dataset with name " + dataSetName, e);
            throw e;
        }
    }
    
    protected PipelineInfo getPipelineInfo(String pipelineName, ClientConfig clientConfig) {
        String pipelineStatus = null;
        Long lastStartEpochTime = null;
        String runId = null;
        RunRecord record;
        try {
            record = getPipelineLastRunInfo(pipelineName, clientConfig);
            runId = record.getPid();
            pipelineStatus = record.getStatus().toString();
            lastStartEpochTime = record.getStartTs();
        } catch (Exception e) {
            ApplicationClient applicationClient = new ApplicationClient(clientConfig);
            ApplicationId appId = new ApplicationId("default", pipelineName);
            try {
                if (applicationClient.exists(appId)) {
                    pipelineStatus = "Deployed";
                } else {
                    pipelineStatus = "Unknown";
                }
            } catch (UnauthorizedException | IOException | UnauthenticatedException e1) {
                LOG.error("Unable to check application existence status for pipeline = " + pipelineName);
                pipelineStatus = "Unknown";
            }
        }
        return new PipelineInfo(pipelineName, "", pipelineStatus, runId, lastStartEpochTime);
    }
    
    protected RunRecord getPipelineLastRunInfo(String pipelineName, ClientConfig clientConfig) throws Exception {
        ProgramClient programClient = new ProgramClient(clientConfig);
        ApplicationId appId = new ApplicationId("default", pipelineName);
        ProgramId programId = new ProgramId(appId, ProgramType.WORKFLOW, "DataPipelineWorkflow");
        List<RunRecord> workflowRuns = programClient.getProgramRuns(programId, ProgramRunStatus.ALL.name(), 0,
                Long.MAX_VALUE, 1);
        return workflowRuns.get(0);
    }
    
    protected Map<String, CDAPPipelineInfo> getWranglerPluginConfigMap(List<String> dataSchemaNames,
            KeyValueTable pluginConfigTable) {
        Map<String, CDAPPipelineInfo> wranglerPluginConfigMap = new HashMap<String, CDAPPipelineInfo>();
        for (String schemaName : dataSchemaNames) {
            byte[] pluginConfigBytes = pluginConfigTable.read(schemaName + "_batch");
            String pluginConfig = new String(pluginConfigBytes, StandardCharsets.UTF_8);
            wranglerPluginConfigMap.put(schemaName, JSONInputParser.parseWranglerPlugin(pluginConfig));
        }
        return wranglerPluginConfigMap;
    }
    
    protected Map<String, NullableSchema> getSchemaMap(final List<String> dataSchemaNames,
            KeyValueTable dataSchemaTable) {
        Map<String, NullableSchema> schemaMap = new HashMap<String, NullableSchema>();
        for (String schemaName : dataSchemaNames) {
            byte[] schemaBytes = dataSchemaTable.read(schemaName);
            String schema = new String(schemaBytes, StandardCharsets.UTF_8);
            schemaMap.put(schemaName, JSONInputParser.parseDataSchemaJSON(schema));
        }
        return schemaMap;
    }
    
}
