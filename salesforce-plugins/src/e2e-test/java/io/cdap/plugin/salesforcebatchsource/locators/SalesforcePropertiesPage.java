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

package io.cdap.plugin.salesforcebatchsource.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Salesforce batch source - Locators.
 */
public class SalesforcePropertiesPage {
  // Salesforce Batch Source - Properties page - Reference section
  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement referenceNameInput;

  // Salesforce Batch Source - Properties page - Authentication section
  @FindBy(how = How.XPATH, using = "//input[@data-cy='username']")
  public static WebElement usernameInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='password']")
  public static WebElement passwordInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='securityToken']")
  public static WebElement securityTokenInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='consumerKey']")
  public static WebElement consumerKeyInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='consumerSecret']")
  public static WebElement consumerSecretInput;

  // Salesforce Batch Source - Properties page - SOQL Query section
  @FindBy(how = How.XPATH, using = "//div[@data-cy='query']//textarea[@data-cy='query']")
  public static WebElement soqlTextarea;

  // Salesforce Batch Source - Properties page - SObject Query section
  @FindBy(how = How.XPATH, using = "//input[@data-cy='sObjectName']")
  public static WebElement sObjectNameInput;
}
