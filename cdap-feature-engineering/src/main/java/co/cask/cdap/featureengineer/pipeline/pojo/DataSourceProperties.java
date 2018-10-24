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
package co.cask.cdap.featureengineer.pipeline.pojo;

/**
 * @author bhupesh.goel
 *
 */
public class DataSourceProperties {

	private String fileSystem;
	private Object fileSystemProperties;
	private String path;
	private String fileRegex;
	private String timeTable;
	private String format;
	private String maxSplitSize;
	private String ignoreNonExistingFolders;
	private String recursive;
	private Schema schema;

	public String getFileSystem() {
		return fileSystem;
	}

	public void setFileSystem(String fileSystem) {
		this.fileSystem = fileSystem;
	}

	public Object getFileSystemProperties() {
		return fileSystemProperties;
	}

	public void setFileSystemProperties(Object fileSystemProperties) {
		this.fileSystemProperties = fileSystemProperties;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getFileRegex() {
		return fileRegex;
	}

	public void setFileRegex(String fileRegex) {
		this.fileRegex = fileRegex;
	}

	public String getTimeTable() {
		return timeTable;
	}

	public void setTimeTable(String timeTable) {
		this.timeTable = timeTable;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getMaxSplitSize() {
		return maxSplitSize;
	}

	public void setMaxSplitSize(String maxSplitSize) {
		this.maxSplitSize = maxSplitSize;
	}

	public String getIgnoreNonExistingFolders() {
		return ignoreNonExistingFolders;
	}

	public void setIgnoreNonExistingFolders(String ignoreNonExistingFolders) {
		this.ignoreNonExistingFolders = ignoreNonExistingFolders;
	}

	public String getRecursive() {
		return recursive;
	}

	public void setRecursive(String recursive) {
		this.recursive = recursive;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	@Override
	public String toString() {
		return "DataSourceProperties [fileSystem=" + fileSystem + ", fileSystemProperties=" + fileSystemProperties
				+ ", path=" + path + ", fileRegex=" + fileRegex + ", timeTable=" + timeTable + ", format=" + format
				+ ", maxSplitSize=" + maxSplitSize + ", ignoreNonExistingFolders=" + ignoreNonExistingFolders
				+ ", recursive=" + recursive + ", schema=" + schema + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fileRegex == null) ? 0 : fileRegex.hashCode());
		result = prime * result + ((fileSystem == null) ? 0 : fileSystem.hashCode());
		result = prime * result + ((fileSystemProperties == null) ? 0 : fileSystemProperties.hashCode());
		result = prime * result + ((format == null) ? 0 : format.hashCode());
		result = prime * result + ((ignoreNonExistingFolders == null) ? 0 : ignoreNonExistingFolders.hashCode());
		result = prime * result + ((maxSplitSize == null) ? 0 : maxSplitSize.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + ((recursive == null) ? 0 : recursive.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		result = prime * result + ((timeTable == null) ? 0 : timeTable.hashCode());
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
		DataSourceProperties other = (DataSourceProperties) obj;
		if (fileRegex == null) {
			if (other.fileRegex != null)
				return false;
		} else if (!fileRegex.equals(other.fileRegex))
			return false;
		if (fileSystem == null) {
			if (other.fileSystem != null)
				return false;
		} else if (!fileSystem.equals(other.fileSystem))
			return false;
		if (fileSystemProperties == null) {
			if (other.fileSystemProperties != null)
				return false;
		} else if (!fileSystemProperties.equals(other.fileSystemProperties))
			return false;
		if (format == null) {
			if (other.format != null)
				return false;
		} else if (!format.equals(other.format))
			return false;
		if (ignoreNonExistingFolders == null) {
			if (other.ignoreNonExistingFolders != null)
				return false;
		} else if (!ignoreNonExistingFolders.equals(other.ignoreNonExistingFolders))
			return false;
		if (maxSplitSize == null) {
			if (other.maxSplitSize != null)
				return false;
		} else if (!maxSplitSize.equals(other.maxSplitSize))
			return false;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (recursive == null) {
			if (other.recursive != null)
				return false;
		} else if (!recursive.equals(other.recursive))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (timeTable == null) {
			if (other.timeTable != null)
				return false;
		} else if (!timeTable.equals(other.timeTable))
			return false;
		return true;
	}

}
