package com.foo.batch.anotherpoc.domain;

public class EtlJobConfiguration {
    private Integer jobId;
    private String jobName;
    private Boolean isOverrideJob; // schedule is not applicable if it is override job
    private String schedule;
    private boolean isActive;


    public EtlJobConfiguration() {
    }

    @Override
    public String toString() {
        return "EtlJobConfiguration{" +
                "jobId=" + jobId +
                ", jobName='" + jobName + '\'' +
                ", isOverrideJob=" + isOverrideJob +
                ", schedule='" + schedule + '\'' +
                ", isActive=" + isActive +
                '}';
    }

    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Boolean getOverrideJob() {
        return isOverrideJob;
    }

    public void setOverrideJob(Boolean overrideJob) {
        isOverrideJob = overrideJob;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }
}
