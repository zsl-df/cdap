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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.featureengineer.FeatureEngineeringApp.FeatureEngineeringConfig;
import co.cask.cdap.featureengineer.response.pojo.PipelineInfo;
import co.cask.cdap.featureengineer.response.pojo.PipelineInfoList;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureEngineeringPipelineServiceHandler extends BaseServiceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(FeatureEngineeringPipelineServiceHandler.class);

	@Property
	private final String pipelineNameTableName;

	private KeyValueTable pipelineNameTable;

	/**
	 * @param config
	 * 
	 */
	public FeatureEngineeringPipelineServiceHandler(FeatureEngineeringConfig config) {
		this.pipelineNameTableName = config.getPipelineNameTable();
	}

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
		pipelineNameTable = context.getDataset(pipelineNameTableName);
	}

	@GET
	@Path("featureengineering/pipeline/getall")
	public void getAllFeatureEngineeringPipelines(HttpServiceRequest request, HttpServiceResponder responder,
			@QueryParam("pipelineType") String pipelineType) {
		CloseableIterator<KeyValue<byte[], byte[]>> iterator = pipelineNameTable.scan(null, null);
		PipelineInfoList pipelineInfoList = new PipelineInfoList();
		while (iterator.hasNext()) {
			KeyValue<byte[], byte[]> keyValue = iterator.next();
			String pipelineName = Bytes.toString(keyValue.getKey());
			String curPipelineType = Bytes.toString(keyValue.getValue());
			if(pipelineType == null || pipelineType.isEmpty()) {
				pipelineInfoList.addPipelineInfo(new PipelineInfo(pipelineName, curPipelineType));
			} else if(pipelineType.equalsIgnoreCase(curPipelineType)) {
				pipelineInfoList.addPipelineInfo(new PipelineInfo(pipelineName, curPipelineType));
			}
		}
		responder.sendJson(pipelineInfoList);
	}

}
