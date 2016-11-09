/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchEmitter;


/**
 * Batch transform detail which wraps stageName, transformation, emitters for output stages
 */
public class BatchTransformDetail {
  private final String stageName;
  private final Transformation transformation;
  private final BatchEmitter<BatchTransformDetail> emitter;
  private final boolean removeStageName;

  public BatchTransformDetail(String stageName, boolean removeStageName,
                              Transformation transformation, BatchEmitter<BatchTransformDetail> emitter) {
    this.stageName = stageName;
    this.removeStageName = removeStageName;
    this.transformation = transformation;
    this.emitter = emitter;
  }

  public void process(KeyValue<String, Object> value) {
    try {
      if (removeStageName) {
        transformation.transform(value.getValue(), emitter);
      } else {
        transformation.transform(value, emitter);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public BatchEmitter<BatchTransformDetail> getEmitter() {
    return emitter;
  }

  public Transformation getTransformation() {
    return transformation;
  }
}
