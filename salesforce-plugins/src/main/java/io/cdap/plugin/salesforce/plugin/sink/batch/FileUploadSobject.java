/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.sink.batch;

/**
 * FileUploadSobject enum with name field and base64 encoded data field.
 */
public enum FileUploadSobject {
  Attachment("Attachment", "Name", "Body"),
  ContentVersion("ContentVersion", "PathOnClient", "VersionData");

  private final String sObjectName;
  private final String nameField;
  private final String dataField;

  FileUploadSobject(String sObjectName, String nameField, String dataField) {
    this.sObjectName = sObjectName;
    this.nameField = nameField;
    this.dataField = dataField;
  }

  public String getsObjectName() {
    return sObjectName;
  }

  public String getNameField() {
    return nameField;
  }

  public String getDataField() {
    return dataField;
  }

  public static boolean isFileUploadSobject(String value) {
    for (FileUploadSobject fileUploadSobject : FileUploadSobject.class.getEnumConstants()) {
      if (fileUploadSobject.name().equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }
}
