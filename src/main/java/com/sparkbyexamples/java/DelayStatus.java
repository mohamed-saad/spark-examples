package com.sparkbyexamples.java;

public class DelayStatus {
    private int countInTime;
    private int countAroundTime;
    private int countDelayed;
    private int countProblem;

    public int getCountInTime() {
        return countInTime;
    }
    public void incCountInTime() {
        countInTime++;
    }
    public void setCountInTime(int countInTime) {
        this.countInTime = countInTime;
    }

    public int getCountAroundTime() {
        return countAroundTime;
    }
    public void incCountAroundTime() {
        countAroundTime++;
    }
    public void setCountAroundTime(int countAroundTime) {
        this.countAroundTime = countAroundTime;
    }

    public int getCountDelayed() {
        return countDelayed;
    }
    public void incCountDelayed() {
        countDelayed++;
    }
    public void setCountDelayed(int countDelayed) {
        this.countDelayed = countDelayed;
    }

    public int getCountProblem() {
        return countProblem;
    }
    public void incCountProblem() {
        countProblem++;
    }
    public void setCountProblem(int countProblem) {
        this.countProblem = countProblem;
    }
}
