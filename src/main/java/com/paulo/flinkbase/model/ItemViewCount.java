package com.paulo.flinkbase.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ItemViewCount {
    /**
     * 商品id
     */
    private long itemId;
    /**
     * 浏览数量
     */
    private long viewCount;

    public  ItemViewCount(){}
}
