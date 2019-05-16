package com.ten.aditum.statistics.service;

import com.ten.aditum.statistics.entity.Record;
import com.ten.aditum.statistics.mapper.RecordDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class RecordService {

    @Resource
    private RecordDao recordDao;

    public int insert(Record pojo) {
        return recordDao.insert(pojo);
    }

    public int insertList(List<Record> pojos) {
        return recordDao.insertList(pojos);
    }

    public List<Record> select(Record pojo) {
        return recordDao.select(pojo);
    }

    public int update(Record pojo) {
        return recordDao.update(pojo);
    }

}
