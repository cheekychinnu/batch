package com.foo.batch.anotherpoc.mapper;

import com.foo.batch.anotherpoc.domain.DataSyncJobMetadata;
import com.foo.batch.anotherpoc.domain.EtlJobConfiguration;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface EtlJobConfigurationMapper {

    String TABLE_NAME = "etl_job_configuration";
    String JOB_TABLE_NAME = "job";

    @Select("select d.job_name, s.is_active from " + TABLE_NAME + " s " +
            "join " + JOB_TABLE_NAME + " d on (d.job_id = s.job_id)")
    List<EtlJobConfiguration> findAll();

    @Select("select is_active from "+TABLE_NAME+" s "+
    "join "+ JOB_TABLE_NAME+" d on (d.job_id = s.job_id) "
    + " where d.job_name = #{jobName}")
    Boolean isActive(String jobName);

    @Update("update "+TABLE_NAME+" s set s.is_active = #{isActive} " +
            "join "+JOB_TABLE_NAME+" d on (d.job_id = s.job_id) where d.job_name = #{jobName}")
    void setIsActive(String jobName, boolean isActive);
}