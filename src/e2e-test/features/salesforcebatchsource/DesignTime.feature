Feature: Salesforce Batch Source - Design time scenarios

  Scenario: Verify user should be able to get output schema for a valid SOQL query
    When I Open CDF Application
    And I select data pipeline type as Data Pipeline - Batch
    And I select data pipeline source as Salesforce
    And I configure Salesforce source for a basic SOQL query "SELECT Id, Name, Phone FROM Account"
    And I click on the Get Schema button
    Then I verify the Output Schema table for fields and types as listed below
    | Field | Type   |
    | Id    | string |
    | Name  | string |
    | Phone | string |

  Scenario: Verify user should be able to get output schema for a valid SObject Name
    When I Open CDF Application
    And I select data pipeline type as Data Pipeline - Batch
    And I select data pipeline source as Salesforce
    And I configure Salesforce source for an SObject Name "Contact"
    And I click on the Validate button
    Then I verify No errors found success message
    And I verify the Output Schema table for fields and types as listed below
      | Field     | Type   |
      | Id        | string |
      | FirstName | string |
      | LastName  | string |