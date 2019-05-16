package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.DeviceAccessHeat;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface DeviceAccessHeatDao {

    int insert(@Param("pojo") DeviceAccessHeat pojo);

    int insertList(@Param("pojos") List<DeviceAccessHeat> pojo);

    List<DeviceAccessHeat> select(@Param("pojo") DeviceAccessHeat pojo);

    int update(@Param("pojo") DeviceAccessHeat pojo);

}
