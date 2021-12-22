package io.cdap.plugin.salesforcebatchsource.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import org.apache.log4j.Logger;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class WaitHelper {
    private static final Logger logger = Logger.getLogger(PluginUtils.class);
    private static final WebDriverWait webDriverWait = SeleniumDriver.waitDriver;

    public static void waitForElementToBeVisible(WebElement element) {
        logger.info("Waiting for the element" + element + "to be visible");
        webDriverWait.until(ExpectedConditions.visibilityOfAllElements(element));
    }

    public static void waitForElementToBeHidden(WebElement element) {
        logger.info("Waiting for the element" + element + "to be hidden");
        webDriverWait.until(ExpectedConditions.invisibilityOf(element));
    }

    public static void waitForElementToBeClickable(WebElement element) {
        logger.info("Waiting for the element" + element + "to be clickable");
        webDriverWait.until(ExpectedConditions.elementToBeClickable(element));
    }
}
