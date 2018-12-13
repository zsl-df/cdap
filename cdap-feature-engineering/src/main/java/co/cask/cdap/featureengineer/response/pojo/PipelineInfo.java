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
package co.cask.cdap.featureengineer.response.pojo;

/**
 * @author bhupesh.goel
 *
 */
public class PipelineInfo {

    private String pipelineName;
    private String pipelineType;

    public PipelineInfo(final String pipelineName, final String pipelineType) {
        this.pipelineName = pipelineName;
        this.pipelineType = pipelineType;
    }

    /**
     * @return the pipelineName
     */
    public String getPipelineName() {
        return pipelineName;
    }

    /**
     * @param pipelineName
     *            the pipelineName to set
     */
    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    /**
     * @return the pipelineType
     */
    public String getPipelineType() {
        return pipelineType;
    }

    /**
     * @param pipelineType
     *            the pipelineType to set
     */
    public void setPipelineType(String pipelineType) {
        this.pipelineType = pipelineType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pipelineName == null) ? 0 : pipelineName.hashCode());
        result = prime * result + ((pipelineType == null) ? 0 : pipelineType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PipelineInfo other = (PipelineInfo) obj;
        if (pipelineName == null) {
            if (other.pipelineName != null) {
                return false;
            }
        } else if (!pipelineName.equals(other.pipelineName)) {
            return false;
        }
        if (pipelineType == null) {
            if (other.pipelineType != null) {
                return false;
            }
        } else if (!pipelineType.equals(other.pipelineType)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PipelineInfo [pipelineName=" + pipelineName + ", pipelineType=" + pipelineType + "]";
    }

}
