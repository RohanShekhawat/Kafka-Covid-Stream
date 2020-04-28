
package com.sid.kafka;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Created by ubu on 29.07.15.
 */
public class TwitterStreamVelocityTask implements StreamTask, WindowableTask {
    private int tweets = 0;

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        tweets++;
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "tweets-vel"), "" + tweets));

        // Reset counts after windowing.
        tweets = 0;
    }

}
