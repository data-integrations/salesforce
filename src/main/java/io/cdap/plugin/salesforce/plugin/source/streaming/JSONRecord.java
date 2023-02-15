/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.source.streaming;
import java.io.Serializable;
import java.util.Map;

/**
 * JSON Record
 */
public class JSONRecord implements Serializable {
    Event event;
    Map<Object, Object> sObject;

    public JSONRecord(Event event, Map<Object, Object> sObject) {
        this.event = event;
        this.sObject = sObject;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public Map<Object, Object> getsObject() {
        return sObject;
    }

    public void setsObject(Map<Object, Object> sObject) {
        this.sObject = sObject;
    }
}

