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

package io.cdap.plugin.salesforcestreamingsource.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSource;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Salesforce Streaming source - Locators.
 */
public class SalesforcePropertiesPage {
    // SalesforceStreaming Source - Properties page
    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'label-input-container')]//input")
    public static WebElement labelInput;

    // SalesforceStreaming  Source - Properties page - Reference section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
    public static WebElement referenceInput;

    // SalesforceStreaming  Source - Properties page - Authentication section
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

    // SalesforceStreaming  Source - Properties page -  PushTopic section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='pushTopicName']")
    public static WebElement topicnameInput;

    @FindBy(how = How.XPATH, using = "//input[@data-cy='pushTopicQuery']")
    public static WebElement topicqueryInput;

    @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn'][./span[contains(text(), 'Get Schema')]]")
    public static WebElement getSchemaButton;

    @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn']//span[contains(@class, 'fa-spin')]")
    public static WebElement loadingSpinnerOnGetSchemaButton;


    @FindBy(how = How.XPATH, using = "//div[@data-cy='select-pushTopicNotifyCreate']")
    public static WebElement notifyoncreateDropdown;

    @FindBy(how = How.XPATH, using = "//div[@data-cy='select-pushTopicNotifyUpdate']")
    public static WebElement notifyonupdateDropdown;

    @FindBy(how = How.XPATH, using = "//div[@data-cy='select-pushTopicNotifyDelete']")
    public static WebElement notifyonDeleteDropdown;

    @FindBy(how = How.XPATH, using = "//div[@data-cy='select-pushTopicNotifyForFields']")
    public static WebElement notifyForFieldsDropdown;


    public static WebElement getDropdownOptionElement(String option) {
        String xpath = "//li[@data-cy='option-" + option + "']";
        return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
    }

    //SalesforceStreaming  Source - Properties page - SObject Query section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='sObjectName']")
    public static WebElement sObjectNameInput;

//For saving and running after adding Runtime Arguments
    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Save & Run')]")
    public static WebElement saveAndRunButton;


}
