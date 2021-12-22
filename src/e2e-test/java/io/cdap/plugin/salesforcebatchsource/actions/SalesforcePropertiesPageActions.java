package io.cdap.plugin.salesforcebatchsource.actions;

import io.cdap.e2e.pages.locators.CdfPipelineRunLocators;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcebatchsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.salesforcebatchsource.stepsdesign.DesignTime;
import io.cdap.plugin.salesforcebatchsource.utils.*;
import io.cucumber.datatable.DataTable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.WebElement;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.cdap.plugin.salesforcebatchsource.utils.PluginUtils.getPluginPropertyValue;

public class SalesforcePropertiesPageActions {
    private static final Logger logger = Logger.getLogger(DesignTime.class);
    public static CdfPipelineRunLocators cdfPipelineRunLocators =
            SeleniumHelper.getPropertiesLocators(CdfPipelineRunLocators.class);

    static {
        SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
    }

    public static void selectDataPipelineTypeAsBatch() {
        WaitHelper.waitForElementToBeClickable(SalesforcePropertiesPage.dataPipelineTypeSelector);
        logger.info("Click on Data Pipeline dropdown");
        SalesforcePropertiesPage.dataPipelineTypeSelector.click();

        logger.info("Select Data Pipeline - Batch option");
        SalesforcePropertiesPage.dataPipelineTypeSelectorBatchOption.click();
    }

    public static void selectSalesforceSourceFromList() {
        ElementHelper.scrollToElement(SalesforcePropertiesPage.salesforceSourceInList);
        logger.info("Click on Salesforce source from the list");
        SalesforcePropertiesPage.salesforceSourceInList.click();
    }

    public static void navigateToSalesforcePropertiesPage() {
        ElementHelper.hoverOverElement(SalesforcePropertiesPage.salesforceSourcePluginTitle);
        WaitHelper.waitForElementToBeClickable(SalesforcePropertiesPage.salesforceSourcePluginPropertiesButton);
        SalesforcePropertiesPage.salesforceSourcePluginPropertiesButton.click();
    }

    public static void fillReferenceName(String referenceName) {
        SalesforcePropertiesPage.referenceInput.sendKeys(referenceName);
    }

    public static void fillAuthenticationProperties(String username, String password, String securityToken, String consumerKey, String consumerSecret) {
        SalesforcePropertiesPage.usernameInput.sendKeys(username);
        SalesforcePropertiesPage.passwordInput.sendKeys(password);
        SalesforcePropertiesPage.securityTokenInput.sendKeys(securityToken);
        SalesforcePropertiesPage.consumerKeyInput.sendKeys(consumerKey);
        SalesforcePropertiesPage.consumerSecretInput.sendKeys(consumerSecret);
    }

    public static void fillAuthenticationPropertiesWithDefaultValues() throws IOException {
        SalesforcePropertiesPageActions.fillAuthenticationProperties(
                getPluginPropertyValue("username"),
                getPluginPropertyValue("password"),
                getPluginPropertyValue("security.token"),
                getPluginPropertyValue("consumer.key"),
                getPluginPropertyValue("consumer.secret"));
    }

    public static void fillSOQLProperty(String soqlQuery) {
        SalesforcePropertiesPage.soqlTextarea.sendKeys(soqlQuery);
    }

    public static void configureSalesforcePluginWithDefaultValuesForSOQL(Boolean navigateTo, String soqlQuery) throws IOException {
        String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);

        if (navigateTo) {
            navigateToSalesforcePropertiesPage();
        }

        fillReferenceName(referenceName);
        fillAuthenticationPropertiesWithDefaultValues();
        fillSOQLProperty(soqlQuery);
    }

    public static void fillSObjectName(String sObjectName) {
        SalesforcePropertiesPage.sObjectNameInput.sendKeys(sObjectName);
    }

    public static void configureSalesforcePluginWithDefaultValuesForSObjectName(Boolean navigateTo, String sObjectName) throws IOException {
        String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);

        if (navigateTo) {
            navigateToSalesforcePropertiesPage();
        }

        fillReferenceName(referenceName);
        fillAuthenticationPropertiesWithDefaultValues();
        fillSObjectName(sObjectName);
    }

    public static void clickOnGetSchemaButton() {
        logger.info("Click on the Get Schema button");
        SalesforcePropertiesPage.getSchemaButton.click();
        WaitHelper.waitForElementToBeVisible(SalesforcePropertiesPage.loadingSpinnerOnGetSchemaButton);
        WaitHelper.waitForElementToBeVisible(SalesforcePropertiesPage.getSchemaButton);
    }

    public static void clickOnValidateButton() {
        logger.info("Click on the Validate button");
        SalesforcePropertiesPage.validateButton.click();
    }

    public static void verifyNoErrorsFoundSuccessMessage() {
        ValidationHelper.verifyElementDisplayed(SalesforcePropertiesPage.noErrorsFoundSuccessMessage);
    }

    public static SchemaTable readExpectedOutputSchemaDetails(DataTable dataTable) {
        List<Map<String, String>> rows = dataTable.asMaps(String.class, String.class);
        SchemaTable schemaTable = new SchemaTable();

        for (Map<String, String> columns : rows) {
            schemaTable.addField(new SchemaFieldTypeMapping(columns.get("Field"), columns.get("Type")));
        }

        return schemaTable;
    }

    public static void verifyFieldTypeMappingDisplayed(SchemaFieldTypeMapping schemaFieldTypeMapping) {
        WebElement element = SalesforcePropertiesPage.getSchemaFieldTypeMappingElement(schemaFieldTypeMapping);
        ValidationHelper.verifyElementDisplayed(element);
    }

    public static void verifyOutputSchemaTable(SchemaTable schemaTable) {
        List<SchemaFieldTypeMapping> listOfFields = schemaTable.getListOfFields();

        for(SchemaFieldTypeMapping schemaFieldTypeMapping: listOfFields) {
            verifyFieldTypeMappingDisplayed(schemaFieldTypeMapping);
        }
    }
}
