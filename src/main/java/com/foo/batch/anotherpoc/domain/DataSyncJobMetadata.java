package com.foo.batch.anotherpoc.domain;

public class DataSyncJobMetadata {
    private String dataset;
    private int rank;

    public DataSyncJobMetadata() {

    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return "DataSyncJobMetadata{" +
                "dataset='" + dataset + '\'' +
                ", rank=" + rank +
                '}';
    }
}
