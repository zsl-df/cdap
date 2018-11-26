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

import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class SelectedFeatureStats {

	private List<FeatureStats> featureStatsList;

	public SelectedFeatureStats() {
		this.featureStatsList = new LinkedList<>();
	}

	/**
	 * @return the featureStatsList
	 */
	public List<FeatureStats> getFeatureStatsList() {
		return featureStatsList;
	}

	/**
	 * @param featureStatsList
	 *            the featureStatsList to set
	 */
	public void setFeatureStatsList(List<FeatureStats> featureStatsList) {
		this.featureStatsList = featureStatsList;
	}

	public void addFeatureStat(FeatureStats featureStat) {
		if (featureStatsList == null) {
			featureStatsList = new LinkedList<>();
		}
		featureStatsList.add(featureStat);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((featureStatsList == null) ? 0 : featureStatsList.hashCode());
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
		SelectedFeatureStats other = (SelectedFeatureStats) obj;
		if (featureStatsList == null) {
			if (other.featureStatsList != null)
				return false;
		} else if (!featureStatsList.equals(other.featureStatsList))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SelectedFeatureStats [featureStatsList=" + featureStatsList + "]";
	}

}
