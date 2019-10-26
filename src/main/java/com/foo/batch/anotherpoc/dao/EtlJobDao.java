package com.foo.batch.anotherpoc.dao;

import com.foo.batch.anotherpoc.dao.mapper.EtlJobConfigurationMapper;
import com.foo.batch.anotherpoc.domain.EtlJobConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class EtlJobDao {

    @Autowired
    private EtlJobConfigurationMapper etlJobConfigurationMapper;

    public boolean isActive(String jobName) {
        return etlJobConfigurationMapper.isActive(jobName);
    }

    public void setIsActive(String jobName, boolean b) {
        etlJobConfigurationMapper.setIsActive(jobName, b);
    }

    public EtlJobConfiguration getEtlConfiguration(String jobName) {
        return etlJobConfigurationMapper.select(jobName);
    }
}
