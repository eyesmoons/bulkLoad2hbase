package com.enbrands.analyze.spark.Hbase.beans;

import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

/**
 * @author shengyu
 * @className SparkColumn
 * @Description SparkColumn
 * @date 2020-08-13 11:50
 **/
public class SparkColumn implements Serializable {

    private String name;

    private String lowerCaseName;

    private DataType dataType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.lowerCaseName = this.name.toLowerCase();
    }

    public String getLowerCaseName() {
        return lowerCaseName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }
}
