package com.foo.batch.anotherpoc.dao;

import com.foo.batch.anotherpoc.dao.mapper.DataSyncJobMetadataMapper;
import com.foo.batch.anotherpoc.domain.DataSyncJobMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DataSyncJobDao {
    @Autowired
    private DataSyncJobMetadataMapper dataSyncJobMetadataMapper;

    public List<DataSyncJobMetadata> findAll() {
        return dataSyncJobMetadataMapper.findAll();
    }
}
