/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.tool.cache;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.util.MockPropertyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MapCacheClient {
    public static DistributedMapCacheClientService createClient(String hostName, String port, String timeout) throws IOException {
        DistributedMapCacheClientService client = new DistributedMapCacheClientService();
        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.PORT, port);
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME, hostName);
        clientProperties.put(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT, timeout);
        CacheConfigurationContext context = new CacheConfigurationContext(clientProperties);
        client.cacheConfig(context);
        return client;
    }


    public static class StringDeserializer implements Deserializer<String> {

        @Override
        public String deserialize(byte[] input) throws DeserializationException, IOException {
            return new String(input);
        }
    }
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class CacheConfigurationContext implements ConfigurationContext {

        public CacheConfigurationContext(Map<PropertyDescriptor, String> properties) {
            this.properties = properties;
        }

        private final Map<PropertyDescriptor, String> properties;

        @Override
        public PropertyValue getProperty(PropertyDescriptor property) {
            String value = properties.get(property);
             return new MockPropertyValue(value, null);
        }

        @Override
        public Map<PropertyDescriptor, String> getProperties() {
             return new HashMap<>(this.properties);
        }

        @Override
        public String getSchedulingPeriod() {
            return null;
        }

        @Override
        public Long getSchedulingPeriod(TimeUnit timeUnit) {
            return null;
        }
    }
}
