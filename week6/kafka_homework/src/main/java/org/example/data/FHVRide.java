package org.example.data;

import java.nio.DoubleBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FHVRide {
    public FHVRide(String[] arr) {
        dispatching_base_num = arr[0];
        pickup_datetime = LocalDateTime.parse(arr[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        dropOff_datetime = LocalDateTime.parse(arr[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        PUlocationID = Long.parseLong(arr[3]);
        DOlocationID = Long.parseLong(arr[4]);
        SR_Flag = arr[5];
        Affiliated_base_number = arr[6];

    }
    public FHVRide(){}
    public String dispatching_base_num;
    public LocalDateTime pickup_datetime;
    public LocalDateTime dropOff_datetime;
    public long PUlocationID;
    public long DOlocationID;
    public String SR_Flag;
    public String Affiliated_base_number ;
    public double fare_amount;
    public double extra;
    public double mta_tax;
    public double tip_amount;
    public double tolls_amount;
    public double improvement_surcharge;
    public double total_amount;
    public String congestion_surcharge;

}
