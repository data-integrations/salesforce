package io.cdap.plugin.salesforcebatchsource.utils;

public class SchemaFieldTypeMapping {
    private final String field;
    private final String type;

    public SchemaFieldTypeMapping(String field, String type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public String getType() {
        return type;
    }
}
