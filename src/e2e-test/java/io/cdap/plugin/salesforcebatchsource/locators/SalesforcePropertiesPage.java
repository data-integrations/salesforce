package io.cdap.plugin.salesforcebatchsource.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.salesforcebatchsource.utils.SchemaFieldTypeMapping;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

public class SalesforcePropertiesPage {

    // Data Pipeline type selector
    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'left-top-section')]//select")
    public static WebElement dataPipelineTypeSelector;

    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'left-top-section')]//select//option[contains(@label, 'Batch')]")
    public static WebElement dataPipelineTypeSelectorBatchOption;

    // List of plugins
    @FindBy(how = How.XPATH, using = "//div[@data-cy='plugin-Salesforce-batchsource']//div[contains(@class, 'plugin-name')][normalize-space(text())= 'Salesforce']")
    public static WebElement salesforceSourceInList;

    // Plugins in the Diagram container
    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'node-name')][@title= 'Salesforce']")
    public static WebElement salesforceSourcePluginTitle;

    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'node-name')][@title= 'Salesforce']/following-sibling::button[@data-cy='node-properties-btn']")
    public static WebElement salesforceSourcePluginPropertiesButton;

    // Salesforce Batch Source - Properties page
    @FindBy(how = How.XPATH, using = "//div[contains(@class, 'label-input-container')]//input")
    public static WebElement labelInput;

    // Salesforce Batch Source - Properties page - Reference section
    @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
    public static WebElement referenceInput;

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
    @FindBy(how = How.XPATH, using = "//textarea[@data-cy='query']")
    public static WebElement soqlTextarea;

    @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn'][./span[contains(text(), 'Get Schema')]]")
    public static WebElement getSchemaButton;

    @FindBy(how = How.XPATH, using = "//button[@data-cy='get-schema-btn']//span[contains(@class, 'fa-spin')]")
    public static WebElement loadingSpinnerOnGetSchemaButton;

    @FindBy(how = How.XPATH, using = "//button[contains(@class, 'validate-btn')]")
    public static WebElement validateButton;

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

    @FindBy(how = How.XPATH, using = "//span[contains(@class, 'text-success')][normalize-space(text())= 'No errors found.']")
    public static WebElement noErrorsFoundSuccessMessage;

    public static WebElement getSchemaFieldTypeMappingElement(SchemaFieldTypeMapping schemaFieldTypeMapping) {
        String xpath = "//div[contains(@data-cy,'schema-row')]//input[@value='" + schemaFieldTypeMapping.getField() + "']/following-sibling::div//select[@title='" + schemaFieldTypeMapping.getType() + "']";
        return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
    }
}
