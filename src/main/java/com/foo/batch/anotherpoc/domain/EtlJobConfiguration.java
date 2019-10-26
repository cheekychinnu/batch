package com.foo.batch.anotherpoc.domain;

public class EtlJobConfiguration {
    private String jobName;
    private boolean isActive;

    public EtlJobConfiguration() {
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    @Override
    public String toString() {
        return "EtlJobConfiguration{" +
                "jobName='" + jobName + '\'' +
                ", isActive=" + isActive +
                '}';
    }
}
