package com.ideal.flume.sink;

import org.apache.flume.Event;

public class EventBo {
    private Event event;
    private String realName;

    public EventBo() {}

    public EventBo(Event event) {
        this.event = event;
    }

    public EventBo(Event event, String realName) {
        this.event = event;
        this.realName = realName;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }


}
