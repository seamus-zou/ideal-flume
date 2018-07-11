package com.ideal.flume.tools.jdbc;


import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.lang3.StringUtils;

import java.beans.PropertyDescriptor;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class GenBeanProcess extends BeanProcessor {

    /**
     * 替换BeanProcessor的映射关系处理
     */
    @Override
    protected int[] mapColumnsToProperties(ResultSetMetaData rsmd, PropertyDescriptor[] props) throws SQLException {
        int cols = rsmd.getColumnCount();
        int[] columnToProperty = new int[cols + 1];
        Arrays.fill(columnToProperty, PROPERTY_NOT_FOUND);
        for (int col = 1; col <= cols; col++) {
            String columnName = rsmd.getColumnLabel(col);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnLabel(col);
            }
            for (int i = 0; i < props.length; i++) {
                if (convert(columnName).equals(props[i].getName())) {
                    columnToProperty[col] = i;
                    break;
                }
            }
        }
        return columnToProperty;
    } /**
     * DATA_OBJECT_NAME -> dataObjectName
     */
    private String convert(String objName) {
        StringBuilder result = new StringBuilder();
        String[] tokens = objName.split("_");
        for (String token : tokens) {
            if (result.length() == 0)
                result.append(token.toLowerCase());
            else
                result.append(StringUtils.capitalize(token.toLowerCase()));
        }
        return result.toString();
    }


}

