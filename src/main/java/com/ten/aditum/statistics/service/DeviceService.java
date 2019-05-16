package com.ten.aditum.statistics.service;

import com.ten.aditum.statistics.entity.Device;
import com.ten.aditum.statistics.mapper.DeviceDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class DeviceService {

    @Resource
    private DeviceDao deviceDao;

    public int insert(Device pojo) {
        return deviceDao.insert(pojo);
    }

    public int insertList(List<Device> pojos) {
        return deviceDao.insertList(pojos);
    }

    public List<Device> select(Device pojo) {
        return deviceDao.select(pojo);
    }

    public int update(Device pojo) {
        return deviceDao.update(pojo);
    }

}
