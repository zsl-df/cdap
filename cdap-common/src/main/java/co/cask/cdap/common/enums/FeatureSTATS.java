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
	
	Feature("Feature", "string", ""),
	Variance ("Variance", "double", "numeric"),
	Max ("Max", "double", "numeric"),
	Min ("Min", "double", "numeric"),
	Mean ("Mean", "double", "numeric"),
	NumOfNonZeros ("No. of Non Zeros", "double", "numeric"),
	NumOfNulls("No. of Nulls", "long", "numeric"),
	NormL1 ("Norm L1", "double", "numeric"),
	NormL2 ("Norm L2", "double", "numeric"),
	TwentyFivePercentile ("25 Percentile", "double", "numeric"),
	FiftyPercentile ("50 Percentile", "double", "numeric"),
	SeventyFivePercentile ("75 Percentile", "double", "numeric"),
	InterQuartilePercentile ("Inter Quartile Percentile", "double", "numeric"),
	LeastFrequentWordCount ("Least Frequent Word Count", "long", "string"),
	TotalCount ("Total Count", "long", "string"),
	UniqueCount ("Unique Count", "long", "string"),
	MostFrequentWordCount ("Most Frequent Word Count", "long", "string"),
	MostFrequentEntry ("Most Frequent Entry", "string", "string");
	
	private final String name;
	private final String type;
	private final String inputType;
	
	private FeatureSTATS(final String name, final String type, final String inputType) {
		this.name = name;
		this.type = type;
		this.inputType = inputType;
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

	/**
	 * @return the inputType
	 */
	public String getInputType() {
		return inputType;
	}
}
