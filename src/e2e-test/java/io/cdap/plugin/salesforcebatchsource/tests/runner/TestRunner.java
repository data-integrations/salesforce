package io.cdap.plugin.salesforcebatchsource.tests.runner;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Test Runner to execute cases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        features = {"src/e2e-test/features/salesforcebatchsource"},
        glue = {"io.cdap.plugin.salesforcebatchsource.stepsdesign", "stepsdesign"},
        tags = {"@SFBATCH"},
        monochrome = true,
        plugin = {"pretty", "html:target/cucumber-html-report", "json:target/cucumber-reports/cucumber.json",
                "junit:target/cucumber-reports/cucumber.xml"}
)
public class TestRunner {
}