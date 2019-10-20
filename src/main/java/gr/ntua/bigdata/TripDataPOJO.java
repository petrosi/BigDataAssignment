package gr.ntua.bigdata;

import java.io.Serializable;
import java.sql.*;

public class TripDataPOJO implements Serializable {
    
    /**
     *
     */
    private static final long serialVersionUID = -6323883228438066884L;

    public Long TripId;

    public Timestamp StartDate;

    public Timestamp EndDate;

    public Double StartLongtitude;

    public Double StartLatitude;

    public Double EndLongtitude;

    public Double EndLatitude;

    public Double Cost;

    public Long getTripId() {
        return TripId;
    }

    public void setTripId(Long tripId) {
        TripId = tripId;
    }

    public Timestamp getStartDate() {
        return StartDate;
    }

    public void setStartDate(Timestamp startDate) {
        StartDate = startDate;
    }

    public Timestamp getEndDate() {
        return EndDate;
    }

    public void setEndDate(Timestamp endDate) {
        EndDate = endDate;
    }

    public Double getStartLongtitude() {
        return StartLongtitude;
    }

    public void setStartLongtitude(Double startLongtitude) {
        StartLongtitude = startLongtitude;
    }

    public Double getStartLatitude() {
        return StartLatitude;
    }

    public void setStartLatitude(Double startLatitude) {
        StartLatitude = startLatitude;
    }

    public Double getEndLongtitude() {
        return EndLongtitude;
    }

    public void setEndLongtitude(Double endLongtitude) {
        EndLongtitude = endLongtitude;
    }

    public Double getEndLatitude() {
        return EndLatitude;
    }

    public void setEndLatitude(Double endLatitude) {
        EndLatitude = endLatitude;
    }

    public Double getCost() {
        return Cost;
    }

    public void setCost(Double cost) {
        Cost = cost;
    }

    public TripDataPOJO(Long tripId, Timestamp startDate, Timestamp endDate, Double startLongtitude,
            Double startLatitude, Double endLongtitude, Double endLatitude, Double cost) {
        TripId = tripId;
        StartDate = startDate;
        EndDate = endDate;
        StartLongtitude = startLongtitude;
        StartLatitude = startLatitude;
        EndLongtitude = endLongtitude;
        EndLatitude = endLatitude;
        Cost = cost;
    }

    public TripDataPOJO() {
    }
    
}