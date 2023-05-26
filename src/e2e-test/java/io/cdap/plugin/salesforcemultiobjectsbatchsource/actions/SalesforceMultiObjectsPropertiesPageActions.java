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

package io.cdap.plugin.salesforcemultiobjectsbatchsource.actions;

import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcemultiobjectsbatchsource.locators.SalesforceMultiObjectsPropertiesPage;
import io.cdap.plugin.utils.enums.SObjects;
import java.util.List;

/**
 * Salesforce MultiObjects batch source plugin - Actions.
 */
public class SalesforceMultiObjectsPropertiesPageActions {
  static {
    SeleniumHelper.getPropertiesLocators(SalesforceMultiObjectsPropertiesPage.class);
  }

  public static void fillWhiteListWithSObjectNames(List<SObjects> sObjectNames) {
    int totalSObjects = sObjectNames.size();

    for (int i = 0; i < totalSObjects - 1; i++) {
      SalesforceMultiObjectsPropertiesPage.sObjectNameAddRowButtonsInWhiteList.get(i).click();
    }

    for (int i = 0; i < totalSObjects; i++) {
      ElementHelper.sendKeys(SalesforceMultiObjectsPropertiesPage.sObjectNameInputsInWhiteList.get(i),
              sObjectNames.get(i).value);
    }
  }

  public static void fillBlackListWithSObjectNames(List<SObjects> sObjectNames) {
    int totalSObjects = sObjectNames.size();

    for (int i = 0; i < totalSObjects - 1; i++) {
      SalesforceMultiObjectsPropertiesPage.sObjectNameAddRowButtonsInBlackList.get(i).click();
    }

    for (int i = 0; i < totalSObjects; i++) {
      ElementHelper.sendKeys(SalesforceMultiObjectsPropertiesPage.sObjectNameInputsInBlackList.get(i),
              sObjectNames.get(i).value);
    }
  }

  public static void verifyInvalidSObjectNameValidationMessageForWhiteList(List<SObjects> whiteListedSObjects) {
    String invalidSObjectNameValidationMessage = PluginPropertyUtils.errorProp("invalid.sobjectnames");

    if (whiteListedSObjects.size() > 0) {
      AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInWhiteList,
        invalidSObjectNameValidationMessage);

      for (SObjects whiteListedSObject : whiteListedSObjects) {
        AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInWhiteList,
          whiteListedSObject.value);
      }
    }
  }

  public static void verifyInvalidSObjectNameValidationMessageForBlackList(List<SObjects> blackListedSObjects) {
    String invalidSObjectNameValidationMessage = PluginPropertyUtils.errorProp("invalid.sobjectnames");

    if (blackListedSObjects.size() > 0) {
      AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInBlackList,
        invalidSObjectNameValidationMessage);

      for (SObjects blackListedSObject : blackListedSObjects) {
        AssertionHelper.verifyElementContainsText(SalesforceMultiObjectsPropertiesPage.propertyErrorInBlackList,
          blackListedSObject.value);
      }
    }
  }
}
