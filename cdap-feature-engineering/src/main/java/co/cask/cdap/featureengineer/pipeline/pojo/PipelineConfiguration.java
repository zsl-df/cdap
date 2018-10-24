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
package co.cask.cdap.featureengineer.pipeline.pojo;

import java.util.List;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public class PipelineConfiguration {

	public List<Connection> connections;
	public String engine;
	public List<BasePipelineNode> postActions;
	public List<String> comments;
	public int instances;
	public Map<String, Integer> clientResources;
	public Map<String, Integer> driverResources;
	public Map<String, Integer> resources;
	public String schedule;
	public List<BasePipelineNode> stages;

	public Map<String, Integer> getClientResources() {
		return clientResources;
	}

	public void setClientResources(Map<String, Integer> clientResources) {
		this.clientResources = clientResources;
	}

	public List<Connection> getConnections() {
		return connections;
	}

	public void setConnections(List<Connection> connections) {
		this.connections = connections;
	}

	public String getEngine() {
		return engine;
	}

	public void setEngine(String engine) {
		this.engine = engine;
	}

	public List<BasePipelineNode> getPostActions() {
		return postActions;
	}

	public void setPostActions(List<BasePipelineNode> postActions) {
		this.postActions = postActions;
	}

	public List<String> getComments() {
		return comments;
	}

	public void setComments(List<String> comments) {
		this.comments = comments;
	}

	public int getInstances() {
		return instances;
	}

	public void setInstances(int instances) {
		this.instances = instances;
	}

	public Map<String, Integer> getResources() {
		return resources;
	}

	public void setResources(Map<String, Integer> resources) {
		this.resources = resources;
	}

	public Map<String, Integer> getDriverResources() {
		return driverResources;
	}

	public void setDriverResources(Map<String, Integer> driverResources) {
		this.driverResources = driverResources;
	}

	public String getSchedule() {
		return schedule;
	}

	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}

	public List<BasePipelineNode> getStages() {
		return stages;
	}

	public void setStages(List<BasePipelineNode> stages) {
		this.stages = stages;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((comments == null) ? 0 : comments.hashCode());
		result = prime * result + ((connections == null) ? 0 : connections.hashCode());
		result = prime * result + ((driverResources == null) ? 0 : driverResources.hashCode());
		result = prime * result + ((resources == null) ? 0 : resources.hashCode());
		result = prime * result + ((engine == null) ? 0 : engine.hashCode());
		result = prime * result + instances;
		result = prime * result + ((postActions == null) ? 0 : postActions.hashCode());
		result = prime * result + ((clientResources == null) ? 0 : clientResources.hashCode());
		result = prime * result + ((schedule == null) ? 0 : schedule.hashCode());
		result = prime * result + ((stages == null) ? 0 : stages.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PipelineConfiguration other = (PipelineConfiguration) obj;
		if (comments == null) {
			if (other.comments != null)
				return false;
		} else if (!comments.equals(other.comments))
			return false;
		if (connections == null) {
			if (other.connections != null)
				return false;
		} else if (!connections.equals(other.connections))
			return false;
		if (driverResources == null) {
			if (other.driverResources != null)
				return false;
		} else if (!driverResources.equals(other.driverResources))
			return false;
		if (resources == null) {
			if (other.resources != null)
				return false;
		} else if (!resources.equals(other.resources))
			return false;
		if (engine == null) {
			if (other.engine != null)
				return false;
		} else if (!engine.equals(other.engine))
			return false;
		if (instances != other.instances)
			return false;
		if (postActions == null) {
			if (other.postActions != null)
				return false;
		} else if (!postActions.equals(other.postActions))
			return false;
		if (clientResources == null) {
			if (other.clientResources != null)
				return false;
		} else if (!clientResources.equals(other.clientResources))
			return false;
		if (schedule == null) {
			if (other.schedule != null)
				return false;
		} else if (!schedule.equals(other.schedule))
			return false;
		if (stages == null) {
			if (other.stages != null)
				return false;
		} else if (!stages.equals(other.stages))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PipelineConfiguration [connections=" + connections + ", engine=" + engine + ", postActions="
				+ postActions + ", comments=" + comments + ", instances=" + instances + ", resources=" + clientResources
				+ ", driverResources=" + driverResources + ", resources=" + resources + ", schedule=" + schedule + ", stages=" + stages + "]";
	}

}
