package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.Device;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface DeviceDao {

    int insert(@Param("pojo") Device pojo);

    int insertList(@Param("pojos") List<Device> pojo);

    List<Device> select(@Param("pojo") Device pojo);

    int update(@Param("pojo") Device pojo);

}
