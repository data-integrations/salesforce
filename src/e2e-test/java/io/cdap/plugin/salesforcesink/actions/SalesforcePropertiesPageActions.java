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

package io.cdap.plugin.salesforcesink.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcesink.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.enums.OperationTypes;
import io.cdap.plugin.utils.enums.SObjects;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Salesforce sink plugins - Actions.
 */

public class SalesforcePropertiesPageActions {

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.referenceInput, referenceName);
  }

  public static void fillsObjectName(String sObjectName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.sObjectNameInput, sObjectName);
  }

  public static void configureSalesforcePluginForSobjectName(SObjects sObjectName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillsObjectName(sObjectName.value);
  }

  public static void selectOperationType(OperationTypes operationType) {
    SalesforcePropertiesPage.locateOperationTypeRadioButtons(operationType.value).click();
    fillUpsertExternalIDIfOperationTypeISUpsert(operationType.value);
  }

  public static void fillUpsertExternalIDIfOperationTypeISUpsert(String operationType) {
    if (operationType.equals(OperationTypes.UPSERT.value)) {
      fillUpsertExternalIdField();
    }
  }

  public static void fillUpsertExternalIdField() {
    String externalId = PluginPropertyUtils.pluginProp("upsert.externalId");
    ElementHelper.sendKeys(SalesforcePropertiesPage.upsertExternalIdFieldInput, externalId);
  }

  public static void selectErrorHandlingOptionType(String errorOption) {
    ElementHelper.selectDropdownOption(SalesforcePropertiesPage.errordropdown,
      CdfPluginPropertiesLocators.locateDropdownListItem(errorOption));
  }

  public static void fillMaxRecords(String maxRecords) {
    ElementHelper.clearElementValue(SalesforcePropertiesPage.maxRecordsPerbatchInput);
    ElementHelper.sendKeys(SalesforcePropertiesPage.maxRecordsPerbatchInput, maxRecords);
  }

  public static void fillMaxBytes(String maxBytesPerBatch) {
    ElementHelper.clearElementValue(SalesforcePropertiesPage.maxBytesPerBatchInput);
    ElementHelper.sendKeys(SalesforcePropertiesPage.maxBytesPerBatchInput, maxBytesPerBatch);
  }
}
