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

package io.cdap.plugin.salesforcestreamingsource.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcestreamingsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Salesforce Streaming source plugins - Actions.
 */
public class SalesforcePropertiesPageActions {

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.referenceInput, referenceName);
  }

  private static void fillTopicName(String topicName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.topicnameInput, topicName);
  }

  public static void configureBasicSection() {
    String referenceName = "TestSF_" + RandomStringUtils.randomAlphanumeric(7);
    String topicName = "TestTopic_" + RandomStringUtils.randomAlphanumeric(5);
    fillReferenceName(referenceName);
    fillTopicName(topicName);
  }

  public static void configureSalesforcePluginForTopicName(String topicName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillTopicName(topicName);
  }

  public static void fillSOQLPropertyField(SOQLQueryType queryType) {
    SalesforcePropertiesPage.topicqueryInput.sendKeys(queryType.query);
  }

  public static void configureSalesforcePluginForPushTopicQuery(SOQLQueryType queryType) {
    fillSOQLPropertyField(queryType);
  }

  public static void selectNotifyOnCreateOption(String onCreateOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.notifyoncreateDropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(onCreateOption));
  }

  public static void selectNotifyOnUpdateOption(String onUpdateOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.notifyonupdateDropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(onUpdateOption));
  }

  public static void selectNotifyOnDeleteOption(String onDeleteOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.notifyonDeleteDropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(onDeleteOption));
  }

  public static void selectNotifyForFieldOption(String forFieldOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.notifyForFieldsDropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(forFieldOption));
  }

  public static void fillUniqueTopicNameInRuntimeArguments(String runtimeArgumentKey) {
    String topicName = "TestTopic" + RandomStringUtils.randomAlphanumeric(7);
    CdfStudioActions.enterRuntimeArgumentValue(runtimeArgumentKey, topicName);
  }
}
