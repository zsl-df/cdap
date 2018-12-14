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

import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class PipelineInfoList {

    private List<PipelineInfo> pipelineInfoList;

    /**
     * 
     */
    public PipelineInfoList() {
        this.pipelineInfoList = new LinkedList<>();
    }

    public void addPipelineInfo(final PipelineInfo pipelineInfo) {
        this.pipelineInfoList.add(pipelineInfo);
    }

    /**
     * @return the pipelineInfoList
     */
    public List<PipelineInfo> getPipelineInfoList() {
        return pipelineInfoList;
    }

    /**
     * @param pipelineInfoList
     *            the pipelineInfoList to set
     */
    public void setPipelineInfoList(List<PipelineInfo> pipelineInfoList) {
        this.pipelineInfoList = pipelineInfoList;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pipelineInfoList == null) ? 0 : pipelineInfoList.hashCode());
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
        PipelineInfoList other = (PipelineInfoList) obj;
        if (pipelineInfoList == null) {
            if (other.pipelineInfoList != null) {
                return false;
            }
        } else if (!pipelineInfoList.equals(other.pipelineInfoList)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PipelineInfoList [pipelineInfoList=" + pipelineInfoList + "]";
    }

}
