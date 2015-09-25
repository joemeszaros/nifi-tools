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


public class MapCacheContainsTool implements Tool {

    @Override
    public String getName() {
        return "map-cache-contains";
    }

    @Override
    public String getShortDescription() {
        return "Determines if the given value is present in the cache and if so returns true, else returns false";
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

        if (client.containsKey(cacheKey, new MapCacheClient.StringSerializer())) {
            out.println("true");
            return 0;
        } else {
            out.println("false");
            return 1;
        }
    }

    private void printHelp(PrintStream ps) {
        ps.println("map-cache-contains --hostname hostname --port port cache-key");
        ps.println();
        ps.println(getShortDescription());
    }
}