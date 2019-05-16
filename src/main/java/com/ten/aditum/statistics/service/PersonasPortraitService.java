package com.ten.aditum.statistics.service;

import com.ten.aditum.statistics.entity.PersonasPortrait;
import com.ten.aditum.statistics.mapper.PersonasPortraitDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class PersonasPortraitService {

    @Resource
    private PersonasPortraitDao personasPortraitDao;

    public int insert(PersonasPortrait pojo) {
        return personasPortraitDao.insert(pojo);
    }

    public int insertList(List<PersonasPortrait> pojos) {
        return personasPortraitDao.insertList(pojos);
    }

    public List<PersonasPortrait> select(PersonasPortrait pojo) {
        return personasPortraitDao.select(pojo);
    }

    public int update(PersonasPortrait pojo) {
        return personasPortraitDao.update(pojo);
    }

}
