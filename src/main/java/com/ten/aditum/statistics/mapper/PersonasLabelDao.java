package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.PersonasLabel;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface PersonasLabelDao {

    int insert(@Param("pojo") PersonasLabel pojo);

    int insertList(@Param("pojos") List<PersonasLabel> pojo);

    List<PersonasLabel> select(@Param("pojo") PersonasLabel pojo);

    int update(@Param("pojo") PersonasLabel pojo);

}
