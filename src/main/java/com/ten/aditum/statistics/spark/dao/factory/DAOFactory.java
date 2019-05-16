package com.ten.aditum.statistics.spark.dao.factory;

import com.ten.aditum.statistics.spark.dao.ITaskDAO;
import com.ten.aditum.statistics.spark.dao.impl.TaskDAOImpl;

/**
 * 数据访问对象工厂类
 */
public class DAOFactory {
    /**
     * 构造并返回TaskDAO实例
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
