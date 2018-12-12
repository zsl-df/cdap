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

import co.cask.cdap.featureengineer.response.pojo.FeatureStats;

/**
 * @author bhupesh.goel
 * @param <T>
 *
 */
public class FeatureStatsPriorityNode<T extends Comparable> implements Comparable<FeatureStatsPriorityNode> {

	private final T value;
	private final FeatureStats featureStats;
	private final boolean ascending;

	public FeatureStatsPriorityNode(T value, FeatureStats featureStats, boolean ascending) {
		this.value = value;
		this.featureStats = featureStats;
		this.ascending = ascending;
	}

	@Override
	public int compareTo(FeatureStatsPriorityNode otherValue) {
		if (ascending) {
			if (value == null) {
				return -1;
			} else if (otherValue.value == null) {
				return 1;
			}
			return value.compareTo(otherValue.value);
		} else {
			if (value == null) {
				return 1;
			} else if (otherValue.value == null) {
				return -1;
			}
			return -value.compareTo(otherValue.value);	
		}
	}

	/**
	 * @return the value
	 */
	public T getValue() {
		return value;
	}

	/**
	 * @return the featureStats
	 */
	public FeatureStats getFeatureStats() {
		return featureStats;
	}

	/**
	 * @return the ascending
	 */
	public boolean isAscending() {
		return ascending;
	}

}
