package com.ten.aditum.statistics.dao.factory;

import com.ten.aditum.statistics.dao.ITaskDAO;
import com.ten.aditum.statistics.dao.impl.TaskDAOImpl;

/**
 * 数据访问对象工厂类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class DAOFactory {
    /**
     * 构造并返回TaskDAO实例
     *
     * @return
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
