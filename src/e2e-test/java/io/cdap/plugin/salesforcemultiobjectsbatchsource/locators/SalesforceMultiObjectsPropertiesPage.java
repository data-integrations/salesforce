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

package io.cdap.plugin.salesforcemultiobjectsbatchsource.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

import java.util.List;

/**
 * Salesforce MultiObjects batch source - Locators.
 */
public class SalesforceMultiObjectsPropertiesPage {

  // SalesforceMultiObjects Batch Source - Properties page - SObject specification section
  @FindBy(how = How.XPATH, using = "//div[@data-cy='whiteList']//input")
  public static List<WebElement> sObjectNameInputsInWhiteList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='whiteList']//button[@data-cy='add-row']")
  public static List<WebElement> sObjectNameAddRowButtonsInWhiteList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='whiteList']//button[@data-cy='remove-row']")
  public static List<WebElement> sObjectNameRemoveRowButtonsInWhiteList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='whiteList']" +
    "//following-sibling::div[contains(@class, 'propertyError')]")
  public static WebElement propertyErrorInWhiteList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='blackList']//input")
  public static List<WebElement> sObjectNameInputsInBlackList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='blackList']//button[@data-cy='add-row']")
  public static List<WebElement> sObjectNameAddRowButtonsInBlackList;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='blackList']" +
    "//following-sibling::div[contains(@class, 'propertyError')]")
  public static WebElement propertyErrorInBlackList;
}
