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
package co.cask.cdap.featureengineer.request.pojo;

/**
 * @author bhupesh.goel
 *
 */
public class StatsFilter {

	private StatsFilterType filterType;
	private String statsName;

	/**
	 * @return the filterType
	 */
	public StatsFilterType getFilterType() {
		return filterType;
	}

	/**
	 * @param filterType
	 *            the filterType to set
	 */
	public void setFilterType(StatsFilterType filterType) {
		this.filterType = filterType;
	}

	/**
	 * @return the statsName
	 */
	public String getStatsName() {
		return statsName;
	}

	/**
	 * @param statsName
	 *            the statsName to set
	 */
	public void setStatsName(String statsName) {
		this.statsName = statsName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filterType == null) ? 0 : filterType.hashCode());
		result = prime * result + ((statsName == null) ? 0 : statsName.hashCode());
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
		StatsFilter other = (StatsFilter) obj;
		if (filterType != other.filterType)
			return false;
		if (statsName == null) {
			if (other.statsName != null)
				return false;
		} else if (!statsName.equals(other.statsName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "StatsFilter [filterType=" + filterType + ", statsName=" + statsName + "]";
	}

}
