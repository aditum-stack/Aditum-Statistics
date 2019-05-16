package com.ten.aditum.statistics.mapper;

import com.ten.aditum.statistics.entity.Person;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface PersonDao {

    int insert(@Param("pojo") Person pojo);

    int insertList(@Param("pojos") List<Person> pojo);

    List<Person> select(@Param("pojo") Person pojo);

    int update(@Param("pojo") Person pojo);

}
