package org.example.data;

public class MyStreamObject {
    public MyStreamObject(String[] arr) {
        type = arr[0];
        data = arr[1];
    }
    public MyStreamObject(){}
    public String type;
    public Long[] data;
}
