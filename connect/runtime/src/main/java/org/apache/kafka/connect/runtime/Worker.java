/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.zkclient.Lock;
import org.apache.kafka.connect.runtime.zkclient.ZkDistributeLock;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * <p>
 * Worker runs a (dynamic) set of tasks in a set of threads, doing the work of actually moving
 * data to/from Kafka.
 * </p>
 * <p>
 * Since each task has a dedicated thread, this is mainly just a container for them.
 * </p>
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final ExecutorService executor;
    private final Time time;
    private final String workerId;
    private final Plugins plugins;
    private final WorkerConfig config;
    private final Converter defaultKeyConverter;
    private final Converter defaultValueConverter;
    private final Converter internalKeyConverter;
    private final Converter internalValueConverter;
    private final OffsetBackingStore offsetBackingStore;
    private final Map<String, Object> producerProps;

    private final ConcurrentMap<String, WorkerConnector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;

    private Lock taskLock;

    public Worker(
            String workerId,
            Time time,
            Plugins plugins,
            WorkerConfig config,
            OffsetBackingStore offsetBackingStore
    ) {
        this.executor = Executors.newCachedThreadPool();
        this.workerId = workerId;
        this.time = time;
        this.plugins = plugins;
        this.config = config;
        // Converters are required properties, thus getClass won't return null.
        this.defaultKeyConverter = plugins.newConverter(
                config.getClass(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG).getName(),
                config
        );
        this.defaultKeyConverter.configure(config.originalsWithPrefix("key.converter."), true);
        this.defaultValueConverter = plugins.newConverter(
                config.getClass(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG).getName(),
                config
        );
        this.defaultValueConverter.configure(config.originalsWithPrefix("value.converter."), false);
        // Same, internal converters are required properties, thus getClass won't return null.
        this.internalKeyConverter = plugins.newConverter(
                config.getClass(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG).getName(),
                config
        );
        this.internalKeyConverter.configure(
                config.originalsWithPrefix("internal.key.converter."),
                true);
        this.internalValueConverter = plugins.newConverter(
                config.getClass(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG).getName(),
                config
        );
        this.internalValueConverter.configure(
                config.originalsWithPrefix("internal.value.converter."),
                false
        );

        this.offsetBackingStore = offsetBackingStore;
        this.offsetBackingStore.configure(config);

        producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings are designed to ensure there is no data loss. They *may* be overridden via configs passed to the
        // worker, but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        // User-specified overrides
        producerProps.putAll(config.originalsWithPrefix("producer."));
        if (config.getBoolean(WorkerConfig.REBALENCE_LOCK_ENABLE)) {
            taskLock = new ZkDistributeLock(config.getString(WorkerConfig.ZOOKEEPER_SERVERS),config.getString(WorkerConfig.ZOKKEPER_ROOT_PATH));
        }
    }

    /**
     * Start worker.
     */
    public void start() {
        log.info("Worker starting");

        offsetBackingStore.start();
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(config);

        log.info("Worker started");
    }

    /**
     * Stop worker.
     */
    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        if (!connectors.isEmpty()) {
            log.warn("Shutting down connectors {} uncleanly; herder should have shut down connectors before the Worker is stopped", connectors.keySet());
            stopConnectors();
        }

        if (!tasks.isEmpty()) {
            log.warn("Shutting down tasks {} uncleanly; herder should have shut down tasks before the Worker is stopped", tasks.keySet());
            stopAndAwaitTasks();
        }

        long timeoutMs = limit - time.milliseconds();
        sourceTaskOffsetCommitter.close(timeoutMs);

        offsetBackingStore.stop();

        log.info("Worker stopped");
    }

    /**
     * Start a connector managed by this worker.
     *
     * @param connName the connector name.
     * @param connProps the properties of the connector.
     * @param ctx the connector runtime context.
     * @param statusListener a listener for the runtime status transitions of the connector.
     * @param initialState the initial state of the connector.
     * @return true if the connector started successfully.
     */
    public boolean startConnector(
            String connName,
            Map<String, String> connProps,
            ConnectorContext ctx,
            ConnectorStatus.Listener statusListener,
            TargetState initialState
    ) {
        if (connectors.containsKey(connName))
            throw new ConnectException("Connector with name " + connName + " already exists");

        final WorkerConnector workerConnector;
        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            final ConnectorConfig connConfig = new ConnectorConfig(plugins, connProps);
            final String connClass = connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            log.info("Creating connector {} of type {}", connName, connClass);
            final Connector connector = plugins.newConnector(connClass);
            workerConnector = new WorkerConnector(connName, connector, ctx, statusListener);
            log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connector.getClass());
            savedLoader = plugins.compareAndSwapLoaders(connector);
            workerConnector.initialize(connConfig);
            workerConnector.transitionTo(initialState);
            Plugins.compareAndSwapLoaders(savedLoader);
        } catch (Throwable t) {
            log.error("Failed to start connector {}", connName, t);
            // Can't be put in a finally block because it needs to be swapped before the call on
            // statusListener
            Plugins.compareAndSwapLoaders(savedLoader);
            statusListener.onFailure(connName, t);
            return false;
        }

        WorkerConnector existing = connectors.putIfAbsent(connName, workerConnector);
        if (existing != null)
            throw new ConnectException("Connector with name " + connName + " already exists");

        log.info("Finished creating connector {}", connName);
        return true;
    }

    /**
     * Return true if the connector associated with this worker is a sink connector.
     *
     * @param connName the connector name.
     * @return true if the connector belongs to the worker and is a sink connector.
     * @throws ConnectException if the worker does not manage a connector with the given name.
     */
    public boolean isSinkConnector(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = plugins.compareAndSwapLoaders(workerConnector.connector());
            return workerConnector.isSinkConnector();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    /**
     * Get a list of updated task properties for the tasks of this connector.
     *
     * @param connName the connector name.
     * @param maxTasks the maxinum number of tasks.
     * @param sinkTopics a list of sink topics.
     * @return a list of updated tasks properties.
     */
    public List<Map<String, String>> connectorTaskConfigs(String connName, int maxTasks, List<String> sinkTopics) {
        log.trace("Reconfiguring connector tasks for {}", connName);

        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        Connector connector = workerConnector.connector();
        List<Map<String, String>> result = new ArrayList<>();
        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = plugins.compareAndSwapLoaders(connector);
            String taskClassName = connector.taskClass().getName();
            for (Map<String, String> taskProps : connector.taskConfigs(maxTasks)) {
                // Ensure we don't modify the connector's copy of the config
                Map<String, String> taskConfig = new HashMap<>(taskProps);
                taskConfig.put(TaskConfig.TASK_CLASS_CONFIG, taskClassName);
                if (sinkTopics != null) {
                    taskConfig.put(SinkTask.TOPICS_CONFIG, Utils.join(sinkTopics, ","));
                }
                result.add(taskConfig);
            }
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }

        return result;
    }

    private void stopConnectors() {
        // Herder is responsible for stopping connectors. This is an internal method to sequentially
        // stop connectors that have not explicitly been stopped.
        for (String connector: connectors.keySet())
            stopConnector(connector);
    }

    /**
     * Stop a connector managed by this worker.
     *
     * @param connName the connector name.
     * @return true if the connector belonged to this worker and was successfully stopped.
     */
    public boolean stopConnector(String connName) {
        log.info("Stopping connector {}", connName);

        WorkerConnector workerConnector = connectors.remove(connName);
        if (workerConnector == null) {
            log.warn("Ignoring stop request for unowned connector {}", connName);
            return false;
        }

        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = plugins.compareAndSwapLoaders(workerConnector.connector());
            workerConnector.shutdown();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }

        log.info("Stopped connector {}", connName);
        return true;
    }

    /**
     * Get the IDs of the connectors currently running in this worker.
     *
     * @return the set of connector IDs.
     */
    public Set<String> connectorNames() {
        return connectors.keySet();
    }

    /**
     * Return true if a connector with the given name is managed by this worker and is currently running.
     *
     * @param connName the connector name.
     * @return true if the connector is running, false if the connector is not running or is not manages by this worker.
     */
    public boolean isRunning(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        return workerConnector != null && workerConnector.isRunning();
    }

    /**
     * Start a task managed by this worker.
     *
     * @param id the task ID.
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param initialState the initial state of the connector.
     * @return true if the task started successfully.
     */
    public boolean startTask(
            ConnectorTaskId id,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState
    ) {
        log.info("Creating task {}", id);

        if (tasks.containsKey(id))
            throw new ConnectException("Task already exists in this worker: " + id);

        final WorkerTask workerTask;
        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            final ConnectorConfig connConfig = new ConnectorConfig(plugins, connProps);
            String connType = connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            ClassLoader connectorLoader = plugins.delegatingLoader().connectorLoader(connType);
            savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);
            final TaskConfig taskConfig = new TaskConfig(taskProps);
            final Class<? extends Task> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(Task.class);
            final Task task = plugins.newTask(taskClass);
            log.info("Instantiated task {} with version {} of type {}", id, task.version(), taskClass.getName());

            // By maintaining connector's specific class loader for this thread here, we first
            // search for converters within the connector dependencies, and if not found the
            // plugin class loader delegates loading to the delegating classloader.
            Converter keyConverter = connConfig.getConfiguredInstance(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Converter.class);
            if (keyConverter != null)
                keyConverter.configure(connConfig.originalsWithPrefix("key.converter."), true);
            else
                keyConverter = defaultKeyConverter;
            Converter valueConverter = connConfig.getConfiguredInstance(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
            if (valueConverter != null)
                valueConverter.configure(connConfig.originalsWithPrefix("value.converter."), false);
            else
                valueConverter = defaultValueConverter;

            workerTask = buildWorkerTask(connConfig, id, task, statusListener, initialState, keyConverter, valueConverter, connectorLoader);
            workerTask.initialize(taskConfig);
            Plugins.compareAndSwapLoaders(savedLoader);
        } catch (Throwable t) {
            log.error("Failed to start task {}", id, t);
            // Can't be put in a finally block because it needs to be swapped before the call on
            // statusListener
            Plugins.compareAndSwapLoaders(savedLoader);
            statusListener.onFailure(id, t);
            return false;
        }

        WorkerTask existing = tasks.putIfAbsent(id, workerTask);
        if (existing != null)
            throw new ConnectException("Task already exists in this worker: " + id);

        executor.submit(workerTask);
        if (workerTask instanceof WorkerSourceTask) {
            sourceTaskOffsetCommitter.schedule(id, (WorkerSourceTask) workerTask);
        }
        return true;
    }

    private WorkerTask buildWorkerTask(ConnectorConfig connConfig,
                                       ConnectorTaskId id,
                                       Task task,
                                       TaskStatus.Listener statusListener,
                                       TargetState initialState,
                                       Converter keyConverter,
                                       Converter valueConverter,
                                       ClassLoader loader) {
        // Decide which type of worker task we need based on the type of task.
        if (task instanceof SourceTask) {
            TransformationChain<SourceRecord> transformationChain = new TransformationChain<>(connConfig.<SourceRecord>transformations());
            OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
            return new WorkerSourceTask(id, (SourceTask) task, statusListener, initialState, keyConverter,
                    valueConverter, transformationChain, producer, offsetReader, offsetWriter, config, loader, time ,taskLock);
        } else if (task instanceof SinkTask) {
            TransformationChain<SinkRecord> transformationChain = new TransformationChain<>(connConfig.<SinkRecord>transformations());
            return new WorkerSinkTask(id, (SinkTask) task, statusListener, initialState, config, keyConverter,
                    valueConverter, transformationChain, loader, time);
        } else {
            log.error("Tasks must be a subclass of either SourceTask or SinkTask", task);
            throw new ConnectException("Tasks must be a subclass of either SourceTask or SinkTask");
        }
    }

    private void stopTask(ConnectorTaskId taskId) {
        WorkerTask task = tasks.get(taskId);
        if (task == null) {
            log.warn("Ignoring stop request for unowned task {}", taskId);
            return;
        }

        log.info("Stopping task {}", task.id());
        if (task instanceof WorkerSourceTask)
            sourceTaskOffsetCommitter.remove(task.id());

        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = Plugins.compareAndSwapLoaders(task.loader());
            task.stop();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    private void stopTasks(Collection<ConnectorTaskId> ids) {
        // Herder is responsible for stopping tasks. This is an internal method to sequentially
        // stop the tasks that have not explicitly been stopped.
        for (ConnectorTaskId taskId : ids) {
            stopTask(taskId);
        }
    }

    private void awaitStopTask(ConnectorTaskId taskId, long timeout) {
        WorkerTask task = tasks.remove(taskId);
        if (task == null) {
            log.warn("Ignoring await stop request for non-present task {}", taskId);
            return;
        }

        if (!task.awaitStop(timeout)) {
            log.error("Graceful stop of task {} failed.", task.id());
            task.cancel();
        }
    }

    private void awaitStopTasks(Collection<ConnectorTaskId> ids) {
        long now = time.milliseconds();
        long deadline = now + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);
        for (ConnectorTaskId id : ids) {
            long remaining = Math.max(0, deadline - time.milliseconds());
            awaitStopTask(id, remaining);
        }
    }

    /**
     * Stop asynchronously all the worker's tasks and await their termination.
     */
    public void stopAndAwaitTasks() {
        stopAndAwaitTasks(new ArrayList<>(tasks.keySet()));
    }

    /**
     * Stop asynchronously a collection of tasks that belong to this worker and await their termination.
     *
     * @param ids the collection of tasks to be stopped.
     */
    public void stopAndAwaitTasks(Collection<ConnectorTaskId> ids) {
        stopTasks(ids);
        awaitStopTasks(ids);
    }

    /**
     * Stop a task that belongs to this worker and await its termination.
     *
     * @param taskId the ID of the task to be stopped.
     */
    public void stopAndAwaitTask(ConnectorTaskId taskId) {
        stopTask(taskId);
        awaitStopTasks(Collections.singletonList(taskId));
    }

    /**
     * Get the IDs of the tasks currently running in this worker.
     */
    public Set<ConnectorTaskId> taskIds() {
        return tasks.keySet();
    }

    public Converter getInternalKeyConverter() {
        return internalKeyConverter;
    }

    public Converter getInternalValueConverter() {
        return internalValueConverter;
    }

    public Plugins getPlugins() {
        return plugins;
    }

    public String workerId() {
        return workerId;
    }

    public void setTargetState(String connName, TargetState state) {
        log.info("Setting connector {} state to {}", connName, state);

        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector != null) {
            ClassLoader connectorLoader =
                    plugins.delegatingLoader().connectorLoader(workerConnector.connector());
            transitionTo(workerConnector, state, connectorLoader);
        }

        for (Map.Entry<ConnectorTaskId, WorkerTask> taskEntry : tasks.entrySet()) {
            if (taskEntry.getKey().connector().equals(connName)) {
                WorkerTask workerTask = taskEntry.getValue();
                transitionTo(workerTask, state, workerTask.loader());
            }
        }
    }

    private void transitionTo(Object connectorOrTask, TargetState state, ClassLoader loader) {
        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = Plugins.compareAndSwapLoaders(loader);
            if (connectorOrTask instanceof WorkerConnector) {
                ((WorkerConnector) connectorOrTask).transitionTo(state);
            } else if (connectorOrTask instanceof WorkerTask) {
                ((WorkerTask) connectorOrTask).transitionTo(state);
            } else {
                throw new ConnectException(
                        "Request for state transition on an object that is neither a "
                                + "WorkerConnector nor a WorkerTask: "
                                + connectorOrTask.getClass());
            }
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }
}
