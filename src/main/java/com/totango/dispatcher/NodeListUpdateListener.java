package com.totango.dispatcher;

import java.util.List;

@FunctionalInterface
public interface NodeListUpdateListener {
    void onNodeListUpdated(List<String> updatedList) throws InterruptedException;
}
