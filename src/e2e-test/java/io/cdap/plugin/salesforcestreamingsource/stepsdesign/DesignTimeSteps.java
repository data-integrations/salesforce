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

package io.cdap.plugin.salesforcestreamingsource.stepsdesign;
import io.cdap.plugin.salesforcestreamingsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.enums.NotifyOptions;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;


/**
 * Design-time steps of Salesforce Streaming plugins.
 */
public class DesignTimeSteps {


    @And("fill Topic Name as: {string}")
    public void fillTopicName(String topicName) {
        SalesforcePropertiesPageActions.configureSalesforcePluginForTopicName(topicName);

    }

    @And("select option for notifyOnCreate as {}")
    public void selectOptionForNotifyOnCreate(NotifyOptions onCreateOption) {
        SalesforcePropertiesPageActions.selectNotifyOnCreateOption(onCreateOption.value);
    }

    @And("select option for notifyOnUpdate as {}")
    public void selectOptionForNotifyOnUpdate(NotifyOptions onUpdateOption) {
        SalesforcePropertiesPageActions.selectNotifyOnUpdateOption(onUpdateOption.value);
    }

    @And("select option for notifyOn Delete as {}")
    public void selectOptionForNotifyOnDelete(NotifyOptions onDeleteOption) {
        SalesforcePropertiesPageActions.selectNotifyOnDeleteOption(onDeleteOption.value);
    }

    @Then("select option for notifyForFields as {}")
    public void selectOptionForNotifyForFields(NotifyOptions forFieldsOption) {
        SalesforcePropertiesPageActions.selectNotifyForFieldOption(forFieldsOption.value);
    }


    @And("configure Salesforce source for an Push Topic Query of type: {string}")
    public void configureSalesforceSourceForPushTopicQuery(String pushTopicQueryType) {
       SalesforcePropertiesPageActions.configureSalesforcePluginForPushTopicQuery
               (SOQLQueryType.valueOf(pushTopicQueryType));
    }
//change
    @And("Click on Save and Run button")
    public void clickOnSaveAndRunButton() {
        SalesforcePropertiesPageActions.clickSaveAndRunButton();
    }

}
