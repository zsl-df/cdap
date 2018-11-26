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
package co.cask.cdap.common.enums;

/**
 * @author bhupesh.goel
 *
 */
public enum FeatureSTATS {
	
	Feature("Feature", "string"),
	Variance ("Variance", "double"),
	Max ("Max", "double"),
	Min ("Min", "double"),
	Mean ("Mean", "double"),
	NumOfNonZeros ("No. of Non Zeros", "double"),
//	NumOfNulls("No. of Nulls", "long"),
	NormL1 ("Norm L1", "double"),
	NormL2 ("Norm L2", "double"),
	TwentyFivePercentile ("25 Percentile", "double"),
	FiftyPercentile ("50 Percentile", "double"),
	SeventyFivePercentile ("75 Percentile", "double"),
	InterQuartilePercentile ("Inter Quartile Percentile", "double"),
	LeastFrequentWordCount ("Least Frequent Word Count", "long"),
	TotalCount ("Total Count", "long"),
	UniqueCount ("Unique Count", "long"),
	MostFrequentWordCount ("Most Frequent Word Count", "long"),
	MostFrequentEntry ("Most Frequent Entry", "string");
	
	private final String name;
	private final String type;
	
	private FeatureSTATS(final String name, final String type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
}
