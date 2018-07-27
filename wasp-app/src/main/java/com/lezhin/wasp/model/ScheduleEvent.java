package com.lezhin.wasp.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author seoeun
 * @since 2018.07.27
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class ScheduleEvent implements Serializable{

    private String name;
    private String status;
    private String type;
    private String result;
    private int ymd;
    private long created_at;
}
