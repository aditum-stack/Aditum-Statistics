package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.DeviceAccessMinuteHeat;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface DeviceAccessMinuteHeatDao {

    int insert(@Param("pojo") DeviceAccessMinuteHeat pojo);

    int insertList(@Param("pojos") List<DeviceAccessMinuteHeat> pojo);

    List<DeviceAccessMinuteHeat> select(@Param("pojo") DeviceAccessMinuteHeat pojo);

    int update(@Param("pojo") DeviceAccessMinuteHeat pojo);

    List<DeviceAccessMinuteHeat> selectOneHourHeat(@Param("pojo") DeviceAccessMinuteHeat pojo);
}
