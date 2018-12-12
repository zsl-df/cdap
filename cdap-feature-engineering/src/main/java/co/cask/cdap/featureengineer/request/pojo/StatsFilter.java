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

import co.cask.cdap.common.enums.FeatureSTATS;

/**
 * @author bhupesh.goel
 *
 */
public class StatsFilter {

	private StatsFilterType filterType;
	private FeatureSTATS statsName;
	private Object lowerLimit;
	private Object upperLimit;

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
	public FeatureSTATS getStatsName() {
		return statsName;
	}

	/**
	 * @param statsName
	 *            the statsName to set
	 */
	public void setStatsName(FeatureSTATS statsName) {
		this.statsName = statsName;
	}

	/**
	 * @return the lowerLimit
	 */
	public Object getLowerLimit() {
		return lowerLimit;
	}

	/**
	 * @param lowerLimit
	 *            the lowerLimit to set
	 */
	public void setLowerLimit(Object lowerLimit) {
		this.lowerLimit = lowerLimit;
	}

	/**
	 * @return the upperLimit
	 */
	public Object getUpperLimit() {
		return upperLimit;
	}

	/**
	 * @param upperLimit
	 *            the upperLimit to set
	 */
	public void setUpperLimit(Object upperLimit) {
		this.upperLimit = upperLimit;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filterType == null) ? 0 : filterType.hashCode());
		result = prime * result + ((lowerLimit == null) ? 0 : lowerLimit.hashCode());
		result = prime * result + ((statsName == null) ? 0 : statsName.hashCode());
		result = prime * result + ((upperLimit == null) ? 0 : upperLimit.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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
		if (lowerLimit == null) {
			if (other.lowerLimit != null)
				return false;
		} else if (!lowerLimit.equals(other.lowerLimit))
			return false;
		if (statsName != other.statsName)
			return false;
		if (upperLimit == null) {
			if (other.upperLimit != null)
				return false;
		} else if (!upperLimit.equals(other.upperLimit))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "StatsFilter [filterType=" + filterType + ", statsName=" + statsName + ", lowerLimit=" + lowerLimit
				+ ", upperLimit=" + upperLimit + "]";
	}

}
