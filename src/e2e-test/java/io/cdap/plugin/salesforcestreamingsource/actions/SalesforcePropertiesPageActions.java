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

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcestreamingsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Salesforce Streaming source plugins - Actions.
 */
public class SalesforcePropertiesPageActions {

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static String topicName;
  public static void fillReferenceName(String referenceName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.referenceInput, referenceName);
  }

  private static void fillTopicName(String topicName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.topicnameInput, topicName);
  }

  public static void configureSalesforcePluginForTopicName() {
    String referenceName = "TestSF_" + RandomStringUtils.randomAlphanumeric(7);
    topicName = "TestTopic_" + RandomStringUtils.randomAlphanumeric(5);
    fillReferenceName(referenceName);
    fillTopicName(topicName);
  }

  public static void fillSOQLPropertyField(SOQLQueryType queryType) {
    SalesforcePropertiesPage.topicqueryInput.sendKeys(queryType.query);
  }

  public static void configureSalesforcePluginForPushTopicQuery(SOQLQueryType queryType) {
    fillSOQLPropertyField(queryType);
  }

  public static void fillUniqueTopicNameInRuntimeArguments(String runtimeArgumentKey) {
    topicName = "TestTopic" + RandomStringUtils.randomAlphanumeric(7);
    CdfStudioActions.enterRuntimeArgumentValue(runtimeArgumentKey, topicName);
  }
}
