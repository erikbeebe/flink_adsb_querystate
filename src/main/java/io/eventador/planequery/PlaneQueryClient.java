/*
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

package io.eventador.planequery;

import java.io.PrintWriter;
import java.util.Optional;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.flink.api.common.ExecutionConfig;

public class PlaneQueryClient {

  public static String STATE_NAME = "faa";

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("Missing required job ID argument. "
          + "Usage: ./PlaneClient <jobID> [jobManagerHost] [jobManagerPort]");
    }
    String jobIdParam = args[0];

    // Configuration
    final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
    final int jobManagerPort = args.length > 1 ? Integer.parseInt(args[1]) : 6123;

    System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);

    final JobID jobId = JobID.fromHexString(jobIdParam);

    final StringSerializer keySerializer = StringSerializer.INSTANCE;
    //final StringSerializer valueSerializer = StringSerializer.INSTANCE;
    TypeSerializer<PlaneModel> valueSerializer = TypeInformation.of(new TypeHint<PlaneModel>(){}).createSerializer(new ExecutionConfig());


    final Time queryTimeout = Time.seconds(15);

    try (
        // This helper is for convenience and not part of Flink
        QueryClientHelper<String, PlaneModel> client = new QueryClientHelper<>(
            jobManagerHost,
            jobManagerPort,
            jobId,
            keySerializer,
            valueSerializer,
            queryTimeout)) {

      printUsage();

      ConsoleReader reader = new ConsoleReader();
      reader.setPrompt("$ ");

      PrintWriter out = new PrintWriter(reader.getOutput());

      String line;
      while ((line = reader.readLine()) != null) {
        //String key = line.toLowerCase().trim();
        String key = line.trim();
        out.printf("[info] Querying key '%s'\n", key);

        try {
          long start = System.currentTimeMillis();
          Optional<PlaneModel> value = client.queryState(STATE_NAME, key);
          long end = System.currentTimeMillis();

          long duration = Math.max(0, end - start);

          if (value.isPresent()) {
            out.printf("%s (query took %d ms)\n", value.get(), duration);
            //out.printf("%s (query took %d ms)\n", value, duration);
          } else {
            out.printf("Unknown key %s (query took %d ms)\n", key, duration);
          }
        } catch (Exception e) {
          out.println("Query failed because of the following Exception:");
          e.printStackTrace(out);
        }
      }
    }
  }

  private static void printUsage() {
    System.out.println("Enter an ICAO key to query.");
    System.out.println();
    System.out.println("The PlaneQueryJob " + STATE_NAME + " state instance.");
    System.out.println();
  }

}
