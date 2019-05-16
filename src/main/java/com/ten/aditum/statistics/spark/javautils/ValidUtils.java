package com.ten.aditum.statistics.spark.javautils;

/**
 * 有效性检验类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

public class ValidUtils {

    /**
     * 检验指定的字段是否在范围内
     *
     * @param data            数据
     * @param dataField       数据字段
     * @param parameter       参数
     * @param startParamField 起始参数字段
     * @param endParamField   结束参数字段
     * @return 校验结果
     */
    public static boolean between(String data, String dataField,
                                  String parameter, String startParamField, String endParamField) {
        String startParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, "\\|", startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, "\\|", endParamField);
        if (startParamFieldStr == null || endParamFieldStr == null) {
            return true;
        }

        int startParamFieldValue = Integer.valueOf(startParamFieldStr);
        int endParamFieldValue = Integer.valueOf(endParamFieldStr);

        String dataFieldStr = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if (dataFieldStr != null) {
            int dataFieldValue = Integer.valueOf(dataFieldStr);
            if (dataFieldValue >= startParamFieldValue &&
                    dataFieldValue <= endParamFieldValue) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    /**
     * 检验指定字段，是否有值与参数字段的值相同
     *
     * @param data       数据
     * @param dataField  数据字段
     * @param parameter  参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean in(String data, String dataField,
                             String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(
                parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }
        String[] paramFieldValueSplited = paramFieldValue.split(",");

        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if (dataFieldValue != null) {
            String[] dataFieldValueSplited = dataFieldValue.split(",");

            for (String singleDataFieldValue : dataFieldValueSplited) {
                for (String singleParamFieldValue : paramFieldValueSplited) {
                    if (singleDataFieldValue.equals(singleParamFieldValue)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 检验指定字段，是否在指定范围内
     *
     * @param data       数据
     * @param dataField  数据字段
     * @param parameter  参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean equal(String data, String dataField,
                                String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(
                parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }

        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if (dataFieldValue != null) {
            if (dataFieldValue.equals(paramFieldValue)) {
                return true;
            }
        }

        return false;
    }
}
