package io.cdap.plugin.salesforcebatchsource.utils;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

public class ValidationHelper {
    private static final Logger logger = Logger.getLogger(PluginUtils.class);

    public static void verifyElementDisplayed(WebElement element) {
        logger.info("Verifying that the element: " + element + " is displayed");
        Assert.assertTrue(element.isDisplayed());
    }
}
