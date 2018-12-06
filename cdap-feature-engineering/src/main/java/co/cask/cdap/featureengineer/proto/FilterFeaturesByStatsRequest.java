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

import co.cask.cdap.featureengineer.request.pojo.CompositeType;
import co.cask.cdap.featureengineer.request.pojo.StatsFilter;

/**
 * @author bhupesh.goel
 *
 */
public class FilterFeaturesByStatsRequest {

	private String orderByStat;
	private int startPosition;
	private int endPosition;
	private boolean isComposite;
	private CompositeType compositeType;

	private List<StatsFilter> filterList;

	/**
	 * @return the orderByStat
	 */
	public String getOrderByStat() {
		return orderByStat;
	}

	/**
	 * @param orderByStat
	 *            the orderByStat to set
	 */
	public void setOrderByStat(String orderByStat) {
		this.orderByStat = orderByStat;
	}

	/**
	 * @return the startPosition
	 */
	public int getStartPosition() {
		return startPosition;
	}

	/**
	 * @param startPosition
	 *            the startPosition to set
	 */
	public void setStartPosition(int startPosition) {
		this.startPosition = startPosition;
	}

	/**
	 * @return the endPosition
	 */
	public int getEndPosition() {
		return endPosition;
	}

	/**
	 * @param endPosition
	 *            the endPosition to set
	 */
	public void setEndPosition(int endPosition) {
		this.endPosition = endPosition;
	}

	/**
	 * @return the isComposite
	 */
	public boolean isComposite() {
		return isComposite;
	}

	/**
	 * @param isComposite
	 *            the isComposite to set
	 */
	public void setComposite(boolean isComposite) {
		this.isComposite = isComposite;
	}

	/**
	 * @return the compositeType
	 */
	public CompositeType getCompositeType() {
		return compositeType;
	}

	/**
	 * @param compositeType
	 *            the compositeType to set
	 */
	public void setCompositeType(CompositeType compositeType) {
		this.compositeType = compositeType;
	}

	/**
	 * @return the filterList
	 */
	public List<StatsFilter> getFilterList() {
		return filterList;
	}

	/**
	 * @param filterList
	 *            the filterList to set
	 */
	public void setFilterList(List<StatsFilter> filterList) {
		this.filterList = filterList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((compositeType == null) ? 0 : compositeType.hashCode());
		result = prime * result + endPosition;
		result = prime * result + ((filterList == null) ? 0 : filterList.hashCode());
		result = prime * result + (isComposite ? 1231 : 1237);
		result = prime * result + ((orderByStat == null) ? 0 : orderByStat.hashCode());
		result = prime * result + startPosition;
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
		FilterFeaturesByStatsRequest other = (FilterFeaturesByStatsRequest) obj;
		if (compositeType != other.compositeType)
			return false;
		if (endPosition != other.endPosition)
			return false;
		if (filterList == null) {
			if (other.filterList != null)
				return false;
		} else if (!filterList.equals(other.filterList))
			return false;
		if (isComposite != other.isComposite)
			return false;
		if (orderByStat == null) {
			if (other.orderByStat != null)
				return false;
		} else if (!orderByStat.equals(other.orderByStat))
			return false;
		if (startPosition != other.startPosition)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FilterFeaturesByStatsRequest [orderByStat=" + orderByStat + ", startPosition=" + startPosition
				+ ", endPosition=" + endPosition + ", isComposite=" + isComposite + ", compositeType=" + compositeType
				+ ", filterList=" + filterList + "]";
	}

}
