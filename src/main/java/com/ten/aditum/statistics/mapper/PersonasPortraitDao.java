package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.PersonasPortrait;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface PersonasPortraitDao {

    int insert(@Param("pojo") PersonasPortrait pojo);

    int insertList(@Param("pojos") List<PersonasPortrait> pojo);

    List<PersonasPortrait> select(@Param("pojo") PersonasPortrait pojo);

    int update(@Param("pojo") PersonasPortrait pojo);

}
