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

package io.cdap.plugin.utils.enums;

import io.cdap.e2e.utils.PluginPropertyUtils;

/**
 * SOQL Query type enum.
 */
public enum SOQLQueryType {
  SIMPLE(PluginPropertyUtils.pluginProp("simple.query")),
  WHERE(PluginPropertyUtils.pluginProp("where.query")),
  GROUPBY(PluginPropertyUtils.pluginProp("groupby.query")),
  PARENTTOCHILD(PluginPropertyUtils.pluginProp("parenttochild.query")),
  CHILDTOPARENT(PluginPropertyUtils.pluginProp("childtoparent.query")),
  STAR(PluginPropertyUtils.pluginProp("star.query"));

  public final String query;

  SOQLQueryType(String query) {
    this.query = query;
  }
}
