package com.foo.batch.anotherpoc.dao.mapper;

import com.foo.batch.anotherpoc.domain.EtlJobConfiguration;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

// TODO: clean up mappers. check the ETL application written on February
@Mapper
public interface EtlJobConfigurationMapper {

    String TABLE_NAME = "etl_job_configuration";

    @Select("select job_id as jobId, job_name as jobName, is_override_job as isOverrideJob, is_active as isActive, schedule as schedule from " + TABLE_NAME)
    List<EtlJobConfiguration> findAll();

    @Select("select is_active from "+TABLE_NAME
    + " where job_name = #{jobName}")
    Boolean isActive(String jobName);

    @Update("update "+TABLE_NAME+"  set is_active = #{isActive} " +
            " where job_name = #{jobName}")
    void setIsActive(String jobName, boolean isActive);

    @Select("select job_id as jobId, job_name as jobName, is_override_job as isOverrideJob, is_active as isActive, schedule as schedule from " + TABLE_NAME
    +" where job_name = #{jobName}")
    EtlJobConfiguration select(String jobName);
}