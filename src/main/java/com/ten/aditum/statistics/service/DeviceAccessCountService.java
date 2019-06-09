package com.ten.aditum.statistics.service;

import com.ten.aditum.statistics.entity.DeviceAccessCount;
import com.ten.aditum.statistics.mapper.DeviceAccessCountDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class DeviceAccessCountService {

    @Resource
    private DeviceAccessCountDao deviceAccessCountDao;

    public int insert(DeviceAccessCount pojo) {
        return deviceAccessCountDao.insert(pojo);
    }

    public int insertList(List<DeviceAccessCount> pojos) {
        return deviceAccessCountDao.insertList(pojos);
    }

    public List<DeviceAccessCount> select(DeviceAccessCount pojo) {
        return deviceAccessCountDao.select(pojo);
    }

    public int update(DeviceAccessCount pojo) {
        return deviceAccessCountDao.update(pojo);
    }

    public List<DeviceAccessCount> selectOneMonth(DeviceAccessCount pojo) {
        return deviceAccessCountDao.selectOneMonth(pojo);
    }
}
