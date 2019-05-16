package com.ten.aditum.statistics.spark.dao;

import com.ten.aditum.statistics.spark.domain.Task;

/**
 * 数据访问对象接口
 */
public interface ITaskDAO {
    Task findById(long taskId);
}
