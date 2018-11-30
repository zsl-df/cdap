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

import co.cask.cdap.featureengineer.request.pojo.ColumnDictionary;
import co.cask.cdap.featureengineer.request.pojo.MultiFieldAggregationInput;
import co.cask.cdap.featureengineer.request.pojo.MultiSchemaColumn;
import co.cask.cdap.featureengineer.request.pojo.Relation;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureGenerationRequest {

	private List<String> dataSchemaNames;
	private List<SchemaColumn> indexes;
	private List<Relation> relationShips;
	private List<SchemaColumn> createEntities;
	private List<SchemaColumn> timestampColumns;
	private List<SchemaColumn> timeIndexColumns;
	private List<SchemaColumn> categoricalColumns;
	private List<SchemaColumn> ignoreColumns;
	private List<ColumnDictionary> categoricalColumnDictionary;
	private List<MultiSchemaColumn> multiFieldTransformationFunctionInputs;
	private List<MultiFieldAggregationInput> multiFieldAggregationFunctionInputs;
	private Integer dfsDepth;
	private List<Integer> trainingWindows;
	private String windowEndTime;
	private String targetEntity;
	private String targetEntityFieldId;
	private String pipelineRunName;

	/**
	 * @return the dataSchemaNames
	 */
	public List<String> getDataSchemaNames() {
		return dataSchemaNames;
	}

	/**
	 * @param dataSchemaNames
	 *            the dataSchemaNames to set
	 */
	public void setDataSchemaNames(List<String> dataSchemaNames) {
		this.dataSchemaNames = dataSchemaNames;
	}

	/**
	 * @return the indexes
	 */
	public List<SchemaColumn> getIndexes() {
		return indexes;
	}

	/**
	 * @param indexes
	 *            the indexes to set
	 */
	public void setIndexes(List<SchemaColumn> indexes) {
		this.indexes = indexes;
	}

	/**
	 * @return the relationShips
	 */
	public List<Relation> getRelationShips() {
		return relationShips;
	}

	/**
	 * @param relationShips
	 *            the relationShips to set
	 */
	public void setRelationShips(List<Relation> relationShips) {
		this.relationShips = relationShips;
	}

	/**
	 * @return the createEntities
	 */
	public List<SchemaColumn> getCreateEntities() {
		return createEntities;
	}

	/**
	 * @param createEntities
	 *            the createEntities to set
	 */
	public void setCreateEntities(List<SchemaColumn> createEntities) {
		this.createEntities = createEntities;
	}

	/**
	 * @return the timestampColumns
	 */
	public List<SchemaColumn> getTimestampColumns() {
		return timestampColumns;
	}

	/**
	 * @param timestampColumns
	 *            the timestampColumns to set
	 */
	public void setTimestampColumns(List<SchemaColumn> timestampColumns) {
		this.timestampColumns = timestampColumns;
	}

	/**
	 * @return the timeIndexColumns
	 */
	public List<SchemaColumn> getTimeIndexColumns() {
		return timeIndexColumns;
	}

	/**
	 * @param timeIndexColumns
	 *            the timeIndexColumns to set
	 */
	public void setTimeIndexColumns(List<SchemaColumn> timeIndexColumns) {
		this.timeIndexColumns = timeIndexColumns;
	}

	/**
	 * @return the categoricalColumns
	 */
	public List<SchemaColumn> getCategoricalColumns() {
		return categoricalColumns;
	}

	/**
	 * @param categoricalColumns
	 *            the categoricalColumns to set
	 */
	public void setCategoricalColumns(List<SchemaColumn> categoricalColumns) {
		this.categoricalColumns = categoricalColumns;
	}

	/**
	 * @return the ignoreColumns
	 */
	public List<SchemaColumn> getIgnoreColumns() {
		return ignoreColumns;
	}

	/**
	 * @param ignoreColumns
	 *            the ignoreColumns to set
	 */
	public void setIgnoreColumns(List<SchemaColumn> ignoreColumns) {
		this.ignoreColumns = ignoreColumns;
	}

	/**
	 * @return the categoricalColumnDictionary
	 */
	public List<ColumnDictionary> getCategoricalColumnDictionary() {
		return categoricalColumnDictionary;
	}

	/**
	 * @param categoricalColumnDictionary
	 *            the categoricalColumnDictionary to set
	 */
	public void setCategoricalColumnDictionary(List<ColumnDictionary> categoricalColumnDictionary) {
		this.categoricalColumnDictionary = categoricalColumnDictionary;
	}

	/**
	 * @return the multiFieldTransformationFunctionInputs
	 */
	public List<MultiSchemaColumn> getMultiFieldTransformationFunctionInputs() {
		return multiFieldTransformationFunctionInputs;
	}

	/**
	 * @param multiFieldTransformationFunctionInputs
	 *            the multiFieldTransformationFunctionInputs to set
	 */
	public void setMultiFieldTransformationFunctionInputs(
			List<MultiSchemaColumn> multiFieldTransformationFunctionInputs) {
		this.multiFieldTransformationFunctionInputs = multiFieldTransformationFunctionInputs;
	}

	/**
	 * @return the multiFieldAggregationFunctionInputs
	 */
	public List<MultiFieldAggregationInput> getMultiFieldAggregationFunctionInputs() {
		return multiFieldAggregationFunctionInputs;
	}

	/**
	 * @param multiFieldAggregationFunctionInputs
	 *            the multiFieldAggregationFunctionInputs to set
	 */
	public void setMultiFieldAggregationFunctionInputs(
			List<MultiFieldAggregationInput> multiFieldAggregationFunctionInputs) {
		this.multiFieldAggregationFunctionInputs = multiFieldAggregationFunctionInputs;
	}

	/**
	 * @return the dfsDepth
	 */
	public Integer getDfsDepth() {
		return dfsDepth;
	}

	/**
	 * @param dfsDepth
	 *            the dfsDepth to set
	 */
	public void setDfsDepth(Integer dfsDepth) {
		this.dfsDepth = dfsDepth;
	}

	/**
	 * @return the trainingWindows
	 */
	public List<Integer> getTrainingWindows() {
		return trainingWindows;
	}

	/**
	 * @param trainingWindows
	 *            the trainingWindows to set
	 */
	public void setTrainingWindows(List<Integer> trainingWindows) {
		this.trainingWindows = trainingWindows;
	}

	/**
	 * @return the windowEndTime
	 */
	public String getWindowEndTime() {
		return windowEndTime;
	}

	/**
	 * @param windowEndTime
	 *            the windowEndTime to set
	 */
	public void setWindowEndTime(String windowEndTime) {
		this.windowEndTime = windowEndTime;
	}

	/**
	 * @return the targetEntity
	 */
	public String getTargetEntity() {
		return targetEntity;
	}

	/**
	 * @param targetEntity
	 *            the targetEntity to set
	 */
	public void setTargetEntity(String targetEntity) {
		this.targetEntity = targetEntity;
	}

	/**
	 * @return the targetEntityFieldId
	 */
	public String getTargetEntityFieldId() {
		return targetEntityFieldId;
	}

	/**
	 * @param targetEntityFieldId
	 *            the targetEntityFieldId to set
	 */
	public void setTargetEntityFieldId(String targetEntityFieldId) {
		this.targetEntityFieldId = targetEntityFieldId;
	}

	/**
	 * @return the pipelineRunName
	 */
	public String getPipelineRunName() {
		return pipelineRunName;
	}

	/**
	 * @param pipelineRunName
	 *            the pipelineRunName to set
	 */
	public void setPipelineRunName(String pipelineRunName) {
		this.pipelineRunName = pipelineRunName;
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
		result = prime * result + ((categoricalColumnDictionary == null) ? 0 : categoricalColumnDictionary.hashCode());
		result = prime * result + ((categoricalColumns == null) ? 0 : categoricalColumns.hashCode());
		result = prime * result + ((createEntities == null) ? 0 : createEntities.hashCode());
		result = prime * result + ((dataSchemaNames == null) ? 0 : dataSchemaNames.hashCode());
		result = prime * result + ((dfsDepth == null) ? 0 : dfsDepth.hashCode());
		result = prime * result + ((ignoreColumns == null) ? 0 : ignoreColumns.hashCode());
		result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
		result = prime * result
				+ ((multiFieldAggregationFunctionInputs == null) ? 0 : multiFieldAggregationFunctionInputs.hashCode());
		result = prime * result + ((multiFieldTransformationFunctionInputs == null) ? 0
				: multiFieldTransformationFunctionInputs.hashCode());
		result = prime * result + ((pipelineRunName == null) ? 0 : pipelineRunName.hashCode());
		result = prime * result + ((relationShips == null) ? 0 : relationShips.hashCode());
		result = prime * result + ((targetEntity == null) ? 0 : targetEntity.hashCode());
		result = prime * result + ((targetEntityFieldId == null) ? 0 : targetEntityFieldId.hashCode());
		result = prime * result + ((timeIndexColumns == null) ? 0 : timeIndexColumns.hashCode());
		result = prime * result + ((timestampColumns == null) ? 0 : timestampColumns.hashCode());
		result = prime * result + ((trainingWindows == null) ? 0 : trainingWindows.hashCode());
		result = prime * result + ((windowEndTime == null) ? 0 : windowEndTime.hashCode());
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
		FeatureGenerationRequest other = (FeatureGenerationRequest) obj;
		if (categoricalColumnDictionary == null) {
			if (other.categoricalColumnDictionary != null)
				return false;
		} else if (!categoricalColumnDictionary.equals(other.categoricalColumnDictionary))
			return false;
		if (categoricalColumns == null) {
			if (other.categoricalColumns != null)
				return false;
		} else if (!categoricalColumns.equals(other.categoricalColumns))
			return false;
		if (createEntities == null) {
			if (other.createEntities != null)
				return false;
		} else if (!createEntities.equals(other.createEntities))
			return false;
		if (dataSchemaNames == null) {
			if (other.dataSchemaNames != null)
				return false;
		} else if (!dataSchemaNames.equals(other.dataSchemaNames))
			return false;
		if (dfsDepth == null) {
			if (other.dfsDepth != null)
				return false;
		} else if (!dfsDepth.equals(other.dfsDepth))
			return false;
		if (ignoreColumns == null) {
			if (other.ignoreColumns != null)
				return false;
		} else if (!ignoreColumns.equals(other.ignoreColumns))
			return false;
		if (indexes == null) {
			if (other.indexes != null)
				return false;
		} else if (!indexes.equals(other.indexes))
			return false;
		if (multiFieldAggregationFunctionInputs == null) {
			if (other.multiFieldAggregationFunctionInputs != null)
				return false;
		} else if (!multiFieldAggregationFunctionInputs.equals(other.multiFieldAggregationFunctionInputs))
			return false;
		if (multiFieldTransformationFunctionInputs == null) {
			if (other.multiFieldTransformationFunctionInputs != null)
				return false;
		} else if (!multiFieldTransformationFunctionInputs.equals(other.multiFieldTransformationFunctionInputs))
			return false;
		if (pipelineRunName == null) {
			if (other.pipelineRunName != null)
				return false;
		} else if (!pipelineRunName.equals(other.pipelineRunName))
			return false;
		if (relationShips == null) {
			if (other.relationShips != null)
				return false;
		} else if (!relationShips.equals(other.relationShips))
			return false;
		if (targetEntity == null) {
			if (other.targetEntity != null)
				return false;
		} else if (!targetEntity.equals(other.targetEntity))
			return false;
		if (targetEntityFieldId == null) {
			if (other.targetEntityFieldId != null)
				return false;
		} else if (!targetEntityFieldId.equals(other.targetEntityFieldId))
			return false;
		if (timeIndexColumns == null) {
			if (other.timeIndexColumns != null)
				return false;
		} else if (!timeIndexColumns.equals(other.timeIndexColumns))
			return false;
		if (timestampColumns == null) {
			if (other.timestampColumns != null)
				return false;
		} else if (!timestampColumns.equals(other.timestampColumns))
			return false;
		if (trainingWindows == null) {
			if (other.trainingWindows != null)
				return false;
		} else if (!trainingWindows.equals(other.trainingWindows))
			return false;
		if (windowEndTime == null) {
			if (other.windowEndTime != null)
				return false;
		} else if (!windowEndTime.equals(other.windowEndTime))
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
		return "FeatureGenerationRequest [dataSchemaNames=" + dataSchemaNames + ", indexes=" + indexes
				+ ", relationShips=" + relationShips + ", createEntities=" + createEntities + ", timestampColumns="
				+ timestampColumns + ", timeIndexColumns=" + timeIndexColumns + ", categoricalColumns="
				+ categoricalColumns + ", ignoreColumns=" + ignoreColumns + ", categoricalColumnDictionary="
				+ categoricalColumnDictionary + ", multiFieldTransformationFunctionInputs="
				+ multiFieldTransformationFunctionInputs + ", multiFieldAggregationFunctionInputs="
				+ multiFieldAggregationFunctionInputs + ", dfsDepth=" + dfsDepth + ", trainingWindows="
				+ trainingWindows + ", windowEndTime=" + windowEndTime + ", targetEntity=" + targetEntity
				+ ", targetEntityFieldId=" + targetEntityFieldId + ", pipelineRunName=" + pipelineRunName + "]";
	}

}
