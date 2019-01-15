package com.king.learn.Flink.streaming.join.bean;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class Element {
    public String name;
    public long number;
    public Element() {
    }

    public Element(String name, long number) {
        this.name = name;
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return this.name + ":" + this.number;
    }
}
