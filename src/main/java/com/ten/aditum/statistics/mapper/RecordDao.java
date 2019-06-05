package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.Record;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface RecordDao {

    int insert(@Param("pojo") Record pojo);

    int insertList(@Param("pojos") List<Record> pojo);

    List<Record> select(@Param("pojo") Record pojo);

    int selectCount(@Param("pojo") Record pojo);

    int selectCountBetweenDateTime(@Param("pojo") Record pojo, @Param("startTime") String startTime, @Param("endTime") String endTime);

    int selectCountAfterDateTime(@Param("pojo") Record pojo, @Param("startTime") String startTime);

    List<Record> selectAfterTheId(@Param("pojo") Record pojo);

    List<Record> selectAfterTheVisitTime(@Param("pojo") Record pojo);

    int update(@Param("pojo") Record pojo);

}
