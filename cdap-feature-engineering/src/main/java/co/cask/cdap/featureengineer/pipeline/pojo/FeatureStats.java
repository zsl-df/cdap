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

import java.util.HashMap;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureStats {

	private String featureName;
	private Map<String, Object> featureStatistics;

	public FeatureStats() {
		this.featureName = "";
		this.featureStatistics = new HashMap<>();
	}

	/**
	 * @return the featureName
	 */
	public String getFeatureName() {
		return featureName;
	}

	/**
	 * @param featureName
	 *            the featureName to set
	 */
	public void setFeatureName(String featureName) {
		this.featureName = featureName;
	}

	public void addFeatureStat(String statName, Object statValue) {
		if (featureStatistics == null) {
			featureStatistics = new HashMap<>();
		}
		featureStatistics.put(statName, statValue);
	}

	/**
	 * @return the featureStatistics
	 */
	public Map<String, Object> getFeatureStatistics() {
		return featureStatistics;
	}

	/**
	 * @param featureStatistics
	 *            the featureStatistics to set
	 */
	public void setFeatureStatistics(Map<String, Object> featureStatistics) {
		this.featureStatistics = featureStatistics;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((featureName == null) ? 0 : featureName.hashCode());
		result = prime * result + ((featureStatistics == null) ? 0 : featureStatistics.hashCode());
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
		FeatureStats other = (FeatureStats) obj;
		if (featureName == null) {
			if (other.featureName != null)
				return false;
		} else if (!featureName.equals(other.featureName))
			return false;
		if (featureStatistics == null) {
			if (other.featureStatistics != null)
				return false;
		} else if (!featureStatistics.equals(other.featureStatistics))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FeatureStats [featureName=" + featureName + ", featureStatistics=" + featureStatistics + "]";
	}

}
