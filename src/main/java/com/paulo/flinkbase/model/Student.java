package com.paulo.flinkbase.model;

import lombok.Data;
import lombok.ToString;

/**
 * @author: create by paulo
 * @version: v1.0
 * @description: com.paulo.flinkbase.model
 * @date:2020/4/15
 */
@Data
@ToString
public class Student {
    /**
     * id
     */
    private int id;
    /**
     * 姓名
     */
    private String name;
    /**
     * 课程
     */
    private String course;
    /**
     * 分数
     */
    private double score;

    public Student(){}
}
