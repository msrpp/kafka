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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Common base class providing configuration for Kafka Connect workers, whether standalone or distributed.
 */
public class WorkerConfig extends AbstractConfig {

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC
            = "A list of host/port pairs to use for establishing the initial connection to the Kafka "
            + "cluster. The client will make use of all servers irrespective of which servers are "
            + "specified here for bootstrapping&mdash;this list only impacts the initial hosts used "
            + "to discover the full set of servers. This list should be in the form "
            + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the "
            + "initial connection to discover the full cluster membership (which may change "
            + "dynamically), this list need not contain the full set of servers (you may want more "
            + "than one, though, in case a server is down).";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    public static final String KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    public static final String VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String INTERNAL_KEY_CONVERTER_CLASS_CONFIG = "internal.key.converter";
    public static final String INTERNAL_KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation.";

    public static final String INTERNAL_VALUE_CONVERTER_CLASS_CONFIG = "internal.value.converter";
    public static final String INTERNAL_VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation.";

    public static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG
            = "task.shutdown.graceful.timeout.ms";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC =
            "Amount of time to wait for tasks to shutdown gracefully. This is the total amount of time,"
                    + " not per task. All task have shutdown triggered, then they are waited on sequentially.";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT = "5000";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms";
    private static final String OFFSET_COMMIT_INTERVAL_MS_DOC
            = "Interval at which to try committing offsets for tasks.";
    public static final long OFFSET_COMMIT_INTERVAL_MS_DEFAULT = 60000L;

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";
    private static final String OFFSET_COMMIT_TIMEOUT_MS_DOC
            = "Maximum number of milliseconds to wait for records to flush and partition offset data to be"
            + " committed to offset storage before cancelling the process and restoring the offset "
            + "data to be committed in a future attempt.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    public static final String REST_HOST_NAME_CONFIG = "rest.host.name";
    private static final String REST_HOST_NAME_DOC
            = "Hostname for the REST API. If this is set, it will only bind to this interface.";

    public static final String REST_PORT_CONFIG = "rest.port";
    private static final String REST_PORT_DOC
            = "Port for the REST API to listen on.";
    public static final int REST_PORT_DEFAULT = 8083;

    public static final String REST_ADVERTISED_HOST_NAME_CONFIG = "rest.advertised.host.name";
    private static final String REST_ADVERTISED_HOST_NAME_DOC
            = "If this is set, this is the hostname that will be given out to other workers to connect to.";

    public static final String REST_ADVERTISED_PORT_CONFIG = "rest.advertised.port";
    private static final String REST_ADVERTISED_PORT_DOC
            = "If this is set, this is the port that will be given out to other workers to connect to.";

    public static final String ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG = "access.control.allow.origin";
    protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DOC =
            "Value to set the Access-Control-Allow-Origin header to for REST API requests." +
                    "To enable cross origin access, set this to the domain of the application that should be permitted" +
                    " to access the API, or '*' to allow access from any domain. The default value only allows access" +
                    " from the domain of the REST API.";
    protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT = "";

    public static final String ACCESS_CONTROL_ALLOW_METHODS_CONFIG = "access.control.allow.methods";
    protected static final String ACCESS_CONTROL_ALLOW_METHODS_DOC =
        "Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header. "
        + "The default value of the Access-Control-Allow-Methods header allows cross origin requests for GET, POST and HEAD.";
    protected static final String ACCESS_CONTROL_ALLOW_METHODS_DEFAULT = "";

    public static final String PLUGIN_PATH_CONFIG = "plugin.path";
    protected static final String PLUGIN_PATH_DOC = "List of paths separated by commas (,) that "
            + "contain plugins (connectors, converters, transformations). The list should consist"
            + " of top level directories that include any combination of: \n"
            + "a) directories immediately containing jars with plugins and their dependencies\n"
            + "b) uber-jars with plugins and their dependencies\n"
            + "c) directories immediately containing the package directory structure of classes of "
            + "plugins and their dependencies\n"
            + "Note: symlinks will be followed to discover dependencies or plugins.\n"
            + "Examples: plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,"
            + "/opt/connectors";
    public static final String REBALENCE_LOCK_ENABLE = "rebalance.lock.enable";

    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";

    public static final String ZOKKEPER_ROOT_PATH = "zookeeper.root.path";

    /**
     * Get a basic ConfigDef for a WorkerConfig. This includes all the common settings. Subclasses can use this to
     * bootstrap their own ConfigDef.
     * @return a ConfigDef with all the common options specified
     */
    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, KEY_CONVERTER_CLASS_DOC)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, VALUE_CONVERTER_CLASS_DOC)
                .define(INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.LOW, INTERNAL_KEY_CONVERTER_CLASS_DOC)
                .define(INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.LOW, INTERNAL_VALUE_CONVERTER_CLASS_DOC)
                .define(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Type.LONG,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT, Importance.LOW,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
                .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC)
                .define(REST_HOST_NAME_CONFIG, Type.STRING, null, Importance.LOW, REST_HOST_NAME_DOC)
                .define(REST_PORT_CONFIG, Type.INT, REST_PORT_DEFAULT, Importance.LOW, REST_PORT_DOC)
                .define(REST_ADVERTISED_HOST_NAME_CONFIG, Type.STRING,  null, Importance.LOW, REST_ADVERTISED_HOST_NAME_DOC)
                .define(REST_ADVERTISED_PORT_CONFIG, Type.INT,  null, Importance.LOW, REST_ADVERTISED_PORT_DOC)
                .define(ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, Type.STRING,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT, Importance.LOW,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DOC)
                .define(ACCESS_CONTROL_ALLOW_METHODS_CONFIG, Type.STRING,
                        ACCESS_CONTROL_ALLOW_METHODS_DEFAULT, Importance.LOW,
                        ACCESS_CONTROL_ALLOW_METHODS_DOC)
                .define(
                        PLUGIN_PATH_CONFIG,
                        Type.LIST,
                        null,
                        Importance.LOW,
                        PLUGIN_PATH_DOC
                )
                .define(REBALENCE_LOCK_ENABLE,Type.BOOLEAN,false,Importance.LOW,"")
                .define(ZOOKEEPER_SERVERS , Type.STRING,null,Importance.LOW,"")
                .define(ZOKKEPER_ROOT_PATH,Type.STRING,null,Importance.LOW,"");
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public static List<String> pluginLocations(Map<String, String> props) {
        String locationList = props.get(WorkerConfig.PLUGIN_PATH_CONFIG);
        return locationList == null
                         ? new ArrayList<String>()
                         : Arrays.asList(locationList.trim().split("\\s*,\\s*", -1));
    }

    public WorkerConfig(ConfigDef definition, Map<String, String> props) {
        super(definition, props);
    }
}
