package io.cdap.plugin.salesforcebatchsource.utils;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PluginUtils {
    private static final Logger logger = Logger.getLogger(PluginUtils.class);
    private static final Properties errorProperties = new Properties();
    public static Properties pluginProperties = new Properties();

    static {
        try {
            errorProperties.load(new FileInputStream("src/e2e-test/resources/error_messages.properties"));
            pluginProperties.load(new FileInputStream("src/e2e-test/resources/plugin_properties.properties"));
        } catch (IOException e) {
            logger.error("Error while reading file: " + e);
        }
    }

    public static String getPluginPropertyValue(String propertyName) {
        return pluginProperties.getProperty(propertyName);
    }

    public static String getErrorPropertyValue(String propertyName) {
        return errorProperties.getProperty(propertyName);
    }
}
