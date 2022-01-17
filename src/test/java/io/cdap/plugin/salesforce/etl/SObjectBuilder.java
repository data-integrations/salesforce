/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.etl;

import com.sforce.soap.partner.sobject.SObject;

import java.util.HashMap;
import java.util.Map;
import javax.xml.namespace.QName;

/**
 * Builder class for {@link com.sforce.soap.partner.sobject.SObject}
 */
public class SObjectBuilder {
  private String type;
  private final Map<String, Object> fields = new HashMap<>();

  public SObjectBuilder setType(String name) {
    this.type = name;
    return this;
  }

  public SObjectBuilder put(String key, Object value) {
    fields.put(key, value);
    return this;
  }

  public SObject build() {
    if (type == null) {
      throw new IllegalArgumentException("SObject type is unset. Unable to build sObject.");
    }
    SObject sobject = new SObject();
    sobject.setType(type);
    sobject.setName(QName.valueOf(type));

    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      sobject.setField(entry.getKey(), entry.getValue());
    }

    return sobject;
  }
}
