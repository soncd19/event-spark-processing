package com.vnpay.ott.spark.entity;

import java.util.List;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayJob {

    private String startDate;
    private String endDate;
    private List<VNPayScheduler> schedulers;

    public VNPayJob(){}

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public List<VNPayScheduler> getSchedulers() {
        return schedulers;
    }

    public void setSchedulers(List<VNPayScheduler> schedulers) {
        this.schedulers = schedulers;
    }
}
