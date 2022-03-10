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

package io.cdap.plugin.salesforcesink.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Salesforce batch sink - Locators.
 */

public class SalesforcePropertiesPage {

    // Salesforce sink - Properties page
    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'label-input-container')]//input")
    public static WebElement labelInput;

    // Salesforce Sink - Properties page - Reference section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
    public static WebElement referenceInput;

    // Salesforce Sink - Properties page - Authentication section
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

    //Salesforce Sink - Properties page - Advanced section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='sObject']")
    public static WebElement sObjectNameInput;

    @FindBy(how = How.XPATH, using = "//input[@data-cy='externalIdField']")
    public static WebElement upsertExternalIdFieldInput;

    @FindBy(how = How.XPATH, using = "//input[@data-cy='maxRecordsPerBatch']")
    public static WebElement maxRecordsPerbatchInput;

    @FindBy(how = How.XPATH, using = "//input[@data-cy='maxBytesPerBatch']")
    public static WebElement maxBytesPerBatchInput;

    @FindBy(how = How.XPATH, using = "//div[@data-cy='select-errorHandling']")
    public static WebElement errordropdown;


    public static WebElement getOperationType(String operationOption) {
        String xpath = "//input[@name='operation' and @value='" + operationOption + "']";
        return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
        }

    public static WebElement getErrorHandlingOptions(String option) {
        String xpath = "//li[@data-value='" + option + "']";
        return SeleniumDriver.getDriver().findElement(By.xpath(xpath));

    }


}
