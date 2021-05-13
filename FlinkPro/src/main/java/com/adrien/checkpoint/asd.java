package com.adrien.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.io.IOException;

public class asd implements StateBackend {

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String s) throws IOException {
        return null;
    }

    @Override
    public CheckpointStorage createCheckpointStorage(JobID jobID) throws IOException {
        return null;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment environment, JobID jobID, String s, TypeSerializer<K> typeSerializer, int i, KeyGroupRange keyGroupRange, TaskKvStateRegistry taskKvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup) throws Exception {
        return null;
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(Environment environment, String s) throws Exception {
        return null;
    }
}
