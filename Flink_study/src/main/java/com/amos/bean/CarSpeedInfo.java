package com.amos.bean;

/**
 * @Title: CarSpeedInfo
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2021/6/22 19:10
 */
public class CarSpeedInfo {
    //310999002004	沪A67R02	2014-08-20 14:09:37	43
    private Long monitorId ;
    private String  carId;
    private String timeStamp ;
    private int speed ;

    public Long getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(Long monitorId) {
        this.monitorId = monitorId;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }
    public CarSpeedInfo(){}
    public CarSpeedInfo(Long monitorId, String carId, String timeStamp, int speed) {
        this.monitorId = monitorId;
        this.carId = carId;
        this.timeStamp = timeStamp;
        this.speed = speed;
    }

    @Override
    public String toString() {
        return "CarSpeedInfo{" +
                "monitorId=" + monitorId +
                ", carId='" + carId + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", speed=" + speed +
                '}';
    }
}
