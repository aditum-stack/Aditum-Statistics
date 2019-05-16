package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.Record;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface RecordDao {

    int insert(@Param("pojo") Record pojo);

    int insertList(@Param("pojos") List<Record> pojo);

    List<Record> select(@Param("pojo") Record pojo);

    int update(@Param("pojo") Record pojo);

}
