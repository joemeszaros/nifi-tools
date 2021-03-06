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

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.tool.Tool;


public class MapCacheGetTool implements Tool {

    @Override
    public String getName() {
        return "map-cache-get";
    }

    @Override
    public String getShortDescription() {
        return "Returns the value in the cache for the given key, if one exists, otherwise returns a warning message.";
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

        OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));

        List<String> nargs = (List<String>)optionSet.nonOptionArguments();

        if (nargs.size() != 1) {
            printHelp(err);
            err.println();
            optionParser.printHelpOn(err);
            return 1;
        }

        String cacheKey = nargs.get(0);

        DistributedMapCacheClientService client = MapCacheClient.createClient(hostName.value(optionSet), port.value(optionSet).toString(), "360 secs");

        String cacheValue = client.get(cacheKey, new MapCacheClient.StringSerializer(), new MapCacheClient.StringDeserializer());

        if (cacheValue == null || cacheValue.isEmpty()) {
            out.println("Not found in cache.");
            return 1;
        } else {
            out.println(cacheValue);
        }

        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println("map-cache-get --hostname hostname --port port cache-key");
        ps.println();
        ps.println(getShortDescription());
    }
}