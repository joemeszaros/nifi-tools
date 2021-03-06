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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.tool.Tool;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;


public class MapCachePutTool implements Tool {

    @Override
    public String getName() {
        return "map-cache-put";
    }

    @Override
    public String getShortDescription() {
        return "Adds the specified key and value to the cache, overwriting any value that is currently set.";
    }

    @Override
    public int run(InputStream stdin, PrintStream out, PrintStream err,
                   List<String> args) throws Exception {
        OptionParser optionParser = new OptionParser();
        OptionSpec<String> hostName =
                optionParser.accepts("hostname", "Hostname")
                        .withRequiredArg()
                        .ofType(String.class);

        OptionSpec<Integer> port =
                optionParser.accepts("port", "Port")
                        .withRequiredArg()
                        .ofType(Integer.class);

        OptionSpec keep_original =
                optionParser.accepts("keep-original", "If the cache entry already exists, do not replace the original value");

        OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));

        List<String> nargs = (List<String>)optionSet.nonOptionArguments();

        if (nargs.size() != 2) {
            printHelp(err);
            err.println();
            optionParser.printHelpOn(err);
            return 1;
        }

        String cacheKey = nargs.get(0);
        String cacheValue = nargs.get(1);

        DistributedMapCacheClientService client = MapCacheClient.createClient(hostName.value(optionSet), port.value(optionSet).toString(), "360 secs");
        if (optionSet.has(keep_original)) {
            boolean existed = client.putIfAbsent(cacheKey, cacheValue, new MapCacheClient.StringSerializer(), new MapCacheClient.StringSerializer());
            if (existed) {
                return 1;
            }
        } else {
            client.put(cacheKey, cacheValue, new MapCacheClient.StringSerializer(), new MapCacheClient.StringSerializer());
        }
        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println("map-cache-put --hostname hostname --port port --keep-original cache-key cache-value");
        ps.println();
        ps.println(getShortDescription());
    }
}