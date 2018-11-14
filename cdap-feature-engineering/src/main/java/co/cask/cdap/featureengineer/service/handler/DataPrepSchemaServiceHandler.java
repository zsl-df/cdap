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

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.featureengineer.FeatureEngineeringApp.FeatureEngineeringConfig;
import co.cask.cdap.featureengineer.RequestExtractor;
import co.cask.cdap.featureengineer.proto.PersistWranglerRequest;

/**
 * @author bhupesh.goel
 *
 */
public class DataPrepSchemaServiceHandler extends BaseServiceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(DataPrepSchemaServiceHandler.class);

	@Property
	private final String dataSchemaTableName;
	@Property
	private final String pluginConfigTableName;

	private KeyValueTable dataSchemaTable;
	private KeyValueTable pluginConfigTable;

	/**
	 * @param config
	 * 
	 */
	public DataPrepSchemaServiceHandler(FeatureEngineeringConfig config) {
		this.dataSchemaTableName = config.getDataSchemaTable();
		this.pluginConfigTableName = config.getPluginConfigTable();
	}

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
		dataSchemaTable = context.getDataset(dataSchemaTableName);
		pluginConfigTable = context.getDataset(pluginConfigTableName);
	}

	@POST
	@Path("featureengineering/{workspaceId}/wrangler/{configType}/config")
	public void persistWranglerPluginConfig(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("workspaceId") String workspaceId, @PathParam("configType") String configType) {
		try {
			PersistWranglerRequest wranglerRequest = new RequestExtractor(request).getContent("UTF-8",
					PersistWranglerRequest.class);
			LOG.debug("Passed pluginConfig is " + wranglerRequest.getPluginConfig());
			dataSchemaTable.write(workspaceId, wranglerRequest.getSchema());
			pluginConfigTable.write(workspaceId + "_" + configType, wranglerRequest.getPluginConfig());
			success(responder, "Successfully persisted wrangler plugin config");
		} catch (Exception e) {
			error(responder, e.getMessage());
		}
	}
}
