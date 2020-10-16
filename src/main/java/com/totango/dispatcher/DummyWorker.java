package com.totango.dispatcher;

import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

@Log4j2
public class DummyWorker implements Worker {

    private AtomicBoolean stop = new AtomicBoolean(false);

    private ConfigDetails configDetails;

    @Override
    public void start() {
        while (!stop.get()) {
            try {
                if (configDetails != null) {
                    log.info(() -> configDetails.toString());
                } else {
                    log.info(() -> "Idle ...");
                }
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error(e, e);
            }

        }
    }

    @Override
    public void workloadUpdated(ConfigDetails configDetails) {
        this.configDetails = configDetails;
    }

    @Override
    public void close() throws IOException {
        stop.set(true);
    }
}
