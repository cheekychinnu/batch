package com.foo.batch.anotherpoc.dao.mapper;

import com.foo.batch.anotherpoc.domain.DataSyncJobMetadata;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface DataSyncJobMetadataMapper {

    String TABLE_NAME = "data_sync_job_metadata";
    String DATASET_TABLE_NAME = "dataset";

    @Select("select d.name as dataset, s.rank from " + TABLE_NAME + " s " +
            "join " + DATASET_TABLE_NAME + " d on (d.dataset_id = s.dataset_id)")
    List<DataSyncJobMetadata> findAll();

}

