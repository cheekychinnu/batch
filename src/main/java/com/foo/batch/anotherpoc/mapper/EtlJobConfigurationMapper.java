package com.foo.batch.anotherpoc.mapper;

import com.foo.batch.anotherpoc.domain.DataSyncJobMetadata;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface EtlJobConfigurationMapper {

    String TABLE_NAME = "etl_job_configuration";
    String JOB_TABLE_NAME = "job";

    @Select("select d.job_name, s.is_active from " + TABLE_NAME + " s " +
            "join " + JOB_TABLE_NAME + " d on (d.job_id = s.job_id)")
    List<DataSyncJobMetadata> findAll();

}