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

/**
 * @author bhupesh.goel
 *
 */
public class PipelineNode extends BasePipelineNode {

	String outputSchema;
	List<InOutSchema> inputSchema;

	public String getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(String outputSchema) {
		this.outputSchema = outputSchema;
	}

	public List<InOutSchema> getInputSchema() {
		return inputSchema;
	}

	public void setInputSchema(List<InOutSchema> inputSchema) {
		this.inputSchema = inputSchema;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((inputSchema == null) ? 0 : inputSchema.hashCode());
		result = prime * result + ((outputSchema == null) ? 0 : outputSchema.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PipelineNode other = (PipelineNode) obj;
		if (inputSchema == null) {
			if (other.inputSchema != null)
				return false;
		} else if (!inputSchema.equals(other.inputSchema))
			return false;
		if (outputSchema == null) {
			if (other.outputSchema != null)
				return false;
		} else if (!outputSchema.equals(other.outputSchema))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PipelineNode [outputSchema=" + outputSchema + ", inputSchema=" + inputSchema + "]";
	}
}
