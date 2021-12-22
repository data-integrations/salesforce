package io.cdap.plugin.salesforcebatchsource.utils;

import java.util.ArrayList;
import java.util.List;

public class SchemaTable {

    private final List<SchemaFieldTypeMapping> listOfFields = new ArrayList<>();

    public void addField(SchemaFieldTypeMapping fieldTypeMapping) {
        listOfFields.add(fieldTypeMapping);
    }

    public List<SchemaFieldTypeMapping> getListOfFields() {
        return listOfFields;
    }
}
