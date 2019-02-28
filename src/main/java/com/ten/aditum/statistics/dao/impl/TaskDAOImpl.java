package com.ten.aditum.statistics.dao.impl;

import com.ten.aditum.statistics.dao.ITaskDAO;
import com.ten.aditum.statistics.domain.Task;
import com.ten.aditum.statistics.jdbc.JDBCHelper;

/**
 * 配置加载管理类
 */
public class TaskDAOImpl implements ITaskDAO {
    /**
     * 构造并返回TaskDAO实例
     *
     * @param taskId
     */
    @Override
    public Task findById(long taskId) {
        final Task task = new Task();
        String sql = "SELECT * FROM task WHERE task_id= ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        jdbcHelper.executeQuery(sql, params, rs -> {
            if (rs.next()) {
                long taskId1 = rs.getLong(1);
                String taskName = rs.getString(2);
                String createTime = rs.getString(3);
                String startTime = rs.getString(4);
                String finishTime = rs.getString(5);
                String taskType = rs.getString(6);
                String taskStatus = rs.getString(7);
                String taskParam = rs.getString(8);
                task.setTaskId(taskId1);
                task.setTaskName(taskName);
                task.setCreateTime(createTime);
                task.setStartTime(startTime);
                task.setFinishTime(finishTime);
                task.setTaskType(taskType);
                task.setTaskStatus(taskStatus);
                task.setTaskParam(taskParam);
            }
        });

        return task;
    }
}