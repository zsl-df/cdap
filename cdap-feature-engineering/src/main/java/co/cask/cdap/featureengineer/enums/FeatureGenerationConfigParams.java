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
package co.cask.cdap.featureengineer.enums;

/**
 * @author bhupesh.goel
 *
 */
public enum FeatureGenerationConfigParams {

    INDEXES("Indexes", true, ""),
    RELATIONSHIPS("Relationships", true, ""),
    TIMESTAMP_COLUMNS("TimestampColumns", true, ""),
    TIME_INDEX_COLUMNS("TimeIndexColumns", true, ""),
    CATEGORICAL_COLUMNS("CategoricalColumns", true, ""), 
    IGNORE_COLUMNS("IgnoreColumns", true, ""),
    MULTI_FIELD_TRANS_FUNCTION_INPUT_COLUMNS("multiFieldTransFunctionInputColumns", true, ""),  
    MULTI_FIELD_AGG_FUNCTION_INPUT_COLUMNS("multiFieldAggFunctionInputColumns", true, ""), 
    TARGET_ENTITY("TargetEntity", true, ""), 
    TARGET_ENTITY_PRIMARY_FIELD("TargetEntityPrimaryField", true, ""),  
    DFS_DEPTH("DFSDepth", false, ""), 
    TRAINING_WINDOWS("TrainingWindows", false, ""),  
    WINDOW_END_TIME("WindowEndTime", false, "");

    final String name;
    final Boolean isSchemaSpecific;
    final String description;

    FeatureGenerationConfigParams(final String name, final boolean isSchemaSpecific, final String description) {
        this.name = name;
        this.isSchemaSpecific = isSchemaSpecific;
        this.description = description;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the isSchemaSpecific
     */
    public Boolean isSchemaSpecific() {
        return isSchemaSpecific;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

}
