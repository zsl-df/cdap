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
package co.cask.cdap.featureengineer.proto;

import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureSelectionRequest {

	private List<String> selectedFeatures;
	private String featureEngineeringPipeline;
	private String featureSelectionPipeline;

	/**
	 * @return the selectedFeatures
	 */
	public List<String> getSelectedFeatures() {
		return selectedFeatures;
	}

	/**
	 * @param selectedFeatures
	 *            the selectedFeatures to set
	 */
	public void setSelectedFeatures(List<String> selectedFeatures) {
		this.selectedFeatures = selectedFeatures;
	}

	/**
	 * @return the featureEngineeringPipeline
	 */
	public String getFeatureEngineeringPipeline() {
		return featureEngineeringPipeline;
	}

	/**
	 * @param featureEngineeringPipeline
	 *            the featureEngineeringPipeline to set
	 */
	public void setFeatureEngineeringPipeline(String featureEngineeringPipeline) {
		this.featureEngineeringPipeline = featureEngineeringPipeline;
	}

	/**
	 * @return the featureSelectionPipeline
	 */
	public String getFeatureSelectionPipeline() {
		return featureSelectionPipeline;
	}

	/**
	 * @param featureSelectionPipeline
	 *            the featureSelectionPipeline to set
	 */
	public void setFeatureSelectionPipeline(String featureSelectionPipeline) {
		this.featureSelectionPipeline = featureSelectionPipeline;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((featureEngineeringPipeline == null) ? 0 : featureEngineeringPipeline.hashCode());
		result = prime * result + ((featureSelectionPipeline == null) ? 0 : featureSelectionPipeline.hashCode());
		result = prime * result + ((selectedFeatures == null) ? 0 : selectedFeatures.hashCode());
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
		FeatureSelectionRequest other = (FeatureSelectionRequest) obj;
		if (featureEngineeringPipeline == null) {
			if (other.featureEngineeringPipeline != null)
				return false;
		} else if (!featureEngineeringPipeline.equals(other.featureEngineeringPipeline))
			return false;
		if (featureSelectionPipeline == null) {
			if (other.featureSelectionPipeline != null)
				return false;
		} else if (!featureSelectionPipeline.equals(other.featureSelectionPipeline))
			return false;
		if (selectedFeatures == null) {
			if (other.selectedFeatures != null)
				return false;
		} else if (!selectedFeatures.equals(other.selectedFeatures))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FeatureSelectionRequest [selectedFeatures=" + selectedFeatures + ", featureEngineeringPipeline="
				+ featureEngineeringPipeline + ", featureSelectionPipeline=" + featureSelectionPipeline + "]";
	}

}
