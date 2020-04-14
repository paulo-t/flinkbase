package com.paulo.flinkbase.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class UserBehavior {
    /**
     *用户id
     */
    private long userId;
    /**
     * 商品id
     */
    private long itemId;
    /**
     * 商品类目
     */
    private int categoryId;
    /**
     *用户行为(pv,buy,cart,fav)
     */
    private String behavior;
    /**
     * 事件发生时间
     */
    private long timestamp;

    public UserBehavior(){}
}
