package io.cdap.plugin.salesforcebatchsource.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.salesforcebatchsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.salesforcebatchsource.utils.SchemaTable;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

public class DesignTime implements CdfHelper {

    @When("I Open CDF Application")
    public void openCDFApplication() throws IOException, InterruptedException {
        openCdf();
    }

    @When("I select data pipeline type as Data Pipeline - Batch")
    public void selectDataPipelineTypeAsBatch() {
        SalesforcePropertiesPageActions.selectDataPipelineTypeAsBatch();
    }

    @When("I select data pipeline source as Salesforce")
    public void selectSalesforceAsSource() {
        SalesforcePropertiesPageActions.selectSalesforceSourceFromList();
    }

    @When("I configure Salesforce source for a basic SOQL query {string}")
    public void configureSalesforceForSOQL(String query) throws IOException {
        SalesforcePropertiesPageActions.configureSalesforcePluginWithDefaultValuesForSOQL(true, query);
    }

    @When("I click on the Get Schema button")
    public void clickOnGetSchemaButton() {
        SalesforcePropertiesPageActions.clickOnGetSchemaButton();
    }

    @When("I configure Salesforce source for an SObject Name {string}")
    public void configureSalesforceForSObjectName(String sObjectName) throws IOException {
        SalesforcePropertiesPageActions.configureSalesforcePluginWithDefaultValuesForSObjectName(true, sObjectName);
    }

    @When("I click on the Validate button")
    public void clickOnValidateButton() {
        SalesforcePropertiesPageActions.clickOnValidateButton();
    }

    @Then("I verify No errors found success message")
    public void verifyNoErrorsFoundSuccessMessage() {
        SalesforcePropertiesPageActions.verifyNoErrorsFoundSuccessMessage();
    }

    @Then("I verify the Output Schema table for fields and types as listed below")
    public void verifyOutputSchemaTable(DataTable dataTable) {
        SchemaTable schemaTable = SalesforcePropertiesPageActions.readExpectedOutputSchemaDetails(dataTable);
        SalesforcePropertiesPageActions.verifyOutputSchemaTable(schemaTable);
    }
}
