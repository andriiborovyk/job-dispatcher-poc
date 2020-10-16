package com.totango.dispatcher;

import java.io.Closeable;

public interface Worker extends Closeable  {

    void start();

    void workloadUpdated(ConfigDetails configDetails);

}
