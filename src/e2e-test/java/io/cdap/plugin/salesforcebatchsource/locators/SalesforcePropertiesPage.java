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

package io.cdap.plugin.salesforcebatchsource.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.utils.SchemaFieldTypeMapping;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Salesforce batch source - Locators.
 */
public class SalesforcePropertiesPage {

  // Salesforce Batch Source - Properties page
  @FindBy(how = How.XPATH, using = "//div[contains(@class, 'label-input-container')]//input")
  public static WebElement labelInput;

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

  @FindBy(how = How.XPATH, using = "//input[@data-cy='loginUrl']")
  public static WebElement loginUrlInput;

  // Salesforce Batch Source - Properties page - SOQL Query section
  @FindBy(how = How.XPATH, using = "//div[@data-cy='query']//textarea[@data-cy='query']")
  public static WebElement soqlTextarea;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn'][./span[contains(text(), 'Get Schema')]]")
  public static WebElement getSchemaButton;

  @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn']//span[contains(@class, 'fa-spin')]")
  public static WebElement loadingSpinnerOnGetSchemaButton;

  // Salesforce Batch Source - Properties page - SObject Query section
  @FindBy(how = How.XPATH, using = "//input[@data-cy='sObjectName']")
  public static WebElement sObjectNameInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='datetimeAfter']")
  public static WebElement lastModifiedAfterInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='datetimeBefore']")
  public static WebElement lastModifiedBeforeInput;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='duration']//div[@data-cy='key']//input")
  public static WebElement durationInput;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='duration']//div[@data-cy='value']//div")
  public static WebElement durationUnitSelector;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='offset']//div[@data-cy='key']//input")
  public static WebElement offsetInput;

  @FindBy(how = How.XPATH, using = "//div[@data-cy='offset']//div[@data-cy='value']//div")
  public static WebElement offsetUnitSelector;

  // Salesforce Batch Source - Properties page - Advanced section
  @FindBy(how = How.XPATH, using = "//div[@data-cy='switch-enablePKChunk']")
  public static WebElement enablePkChunkingToggle;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='chunkSize']")
  public static WebElement chunkSizeInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='parent']")
  public static WebElement sObjectParentNameInput;


  @FindBy(how = How.XPATH, using = "//a[./span[contains(@class, 'fa-remove')]]")
  public static WebElement closePropertiesPageButton;


  public static WebElement getSchemaFieldTypeMappingElement(SchemaFieldTypeMapping schemaFieldTypeMapping) {
    String xpath = "//div[contains(@data-cy,'schema-row')]" +
      "//input[@value='" + schemaFieldTypeMapping.getField() + "']" +
      "/following-sibling::div//select[@title='" + schemaFieldTypeMapping.getType() + "']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  ///
  @FindBy(how = How.XPATH, using = "//div[@data-cy='preview-config-btn']")
  public static WebElement previewConfigRunButton;


}
