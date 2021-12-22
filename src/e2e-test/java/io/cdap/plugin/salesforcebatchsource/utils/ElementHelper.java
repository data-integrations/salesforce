package io.cdap.plugin.salesforcebatchsource.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import org.apache.log4j.Logger;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;

public class ElementHelper {
    private static final Logger logger = Logger.getLogger(PluginUtils.class);
    private static final JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    private static final Actions actions = new Actions(SeleniumDriver.getDriver());

    public static void scrollToElement(WebElement element) {
        logger.info("Scrolling to the element: " + element);
        js.executeScript("arguments[0].scrollIntoView(true);", element);
    }

    public static void hoverOverElement(WebElement element) {
        logger.info("Hovering over the element: " + element);
        actions.moveToElement(element).build().perform();
    }
}
