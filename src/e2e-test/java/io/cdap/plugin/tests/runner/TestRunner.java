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

package io.cdap.plugin.tests.runner;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Test Runner to execute Salesforce plugin's test scenarios.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
  features = {"src/e2e-test/features"},
  glue = {"io.cdap.plugin.salesforcebatchsource.stepsdesign",
    "io.cdap.plugin.salesforcesink.stepsdesign",
    "io.cdap.plugin.salesforcestreamingsource.stepsdesign",
    "io.cdap.plugin.salesforcemultiobjectsbatchsource.stepsdesign",
    "io.cdap.plugin.bigquery.stepsdesign",
    "io.cdap.plugin.tests.hooks", "stepsdesign"},
  tags = {"@Regression"},
  monochrome = true,
  plugin = {"pretty", "html:target/cucumber-html-report", "json:target/cucumber-reports/cucumber.json",
    "junit:target/cucumber-reports/cucumber.xml"}
)

public class TestRunner {
}
