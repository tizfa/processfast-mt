/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * *******************
 */

package it.cnr.isti.hlt.processfast_gpars.test

import it.cnr.isti.hlt.processfast.connector.ConnectorCapability
import it.cnr.isti.hlt.processfast.connector.ConnectorType
import it.cnr.isti.hlt.processfast.connector.ConnectorWriter
import it.cnr.isti.hlt.processfast.core.ProcessfastRuntime
import it.cnr.isti.hlt.processfast.core.TaskContext
import it.cnr.isti.hlt.processfast.core.TaskSet
import it.cnr.isti.hlt.processfast_gpars.core.GParsRuntime
import twitter4j.*

/**
 * Start analyzing the stream of tweets. We use Twitter4J library to query
 * Twitter servers.
 */
void receiveTwitterStream(TaskContext ctx, ConnectorWriter connector) {
    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
    StatusListener listener = new StatusListener() {
        @Override
        public void onStatus(Status status) {
            connector.putValue(status.getText());
        }

        @Override
        public void onDeletionNotice(
                StatusDeletionNotice statusDeletionNotice) {
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }

        @Override
        public void onException(Exception ex) {
            ex.printStackTrace();
        }
    };
    twitterStream.addListener(listener);
    def fq = new FilterQuery()
    String[] languages = ["en"]
    String[] keywords = ["apple"]
    fq = fq.language(languages)
            .track(keywords)
    twitterStream.filter(fq)
}


TaskSet createMainTasksSet(ProcessfastRuntime runtime) {
    // Create main tasks set.
    def ts = runtime.createTaskSet()

    // Create the required connectors.
    ts.createConnector("DISTRIBUTOR", ConnectorType.LOAD_BALANCING_QUEUE)
    ts.createConnector("COLLECTOR", ConnectorType.LOAD_BALANCING_QUEUE)

    // Define the distributor process.
    ts.task { tc ->
        def connector = tc.connectorManager.getConnectorWriter("DISTRIBUTOR")
        receiveTwitterStream(tc, connector);
    }.withConnectors { wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.WRITE) }
            .withName { idInstance -> "Distributor" }.withNumInstances(1, 1)

    // Declare the  worker.
    ts.task { tc ->
        def dist = tc.connectorManager.getConnectorReader("DISTRIBUTOR")
        def coll = tc.connectorManager.getConnectorWriter("COLLECTOR")
        while (true) {
            // Read next tweet.
            String tweet = dist.value.payload

            String[] words = tweet
                    .split("([\\s]+)|([\\:\\.\\,\\;\"\\<\\>\\[\\]\\{\\}\\\\/'\\\\&\\*\\(\\)\\=\\?\\^\\!\\|])");
            int numHashtags = 0;
            for (int i = 0; i < words.length; i++) {
                if (words[i].startsWith("#")) {
                    numHashtags++;
                }
            }
            int tweetLength = tweet.length();
            coll.putValue([numHashTags: numHashtags, tweetLength: tweetLength])
        }
    }.withNumInstances(5, 5).withName { ni -> "TwitterAnalyzer${ni + 1}" }
            .withConnectors { wci ->
        wci.connectorManager.attachTaskToConnector(wci.taskName, "DISTRIBUTOR", ConnectorCapability.READ)
        wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.WRITE)
    }

    // Define the collector.
    ts.task { tc ->
        double avgTweetLength = 0
        double avgNumHashTags = 0
        def coll = tc.connectorManager.getConnectorReader("COLLECTOR")
        def processed = 1
        while (true) {
            def dict = coll.value.payload
            avgTweetLength = avgTweetLength + (dict.tweetLength - avgTweetLength) / processed
            avgNumHashTags = avgNumHashTags + (dict.numHashTags - avgNumHashTags) / processed
            processed++
            if (processed % 20 == 0)
                tc.getLogManager().getLogger("Results").info("Status: average tweet length [${avgTweetLength}], average number hashtags [${avgNumHashTags}]")
        }

    }.withConnectors { wci -> wci.connectorManager.attachTaskToConnector(wci.taskName, "COLLECTOR", ConnectorCapability.READ) }
            .withName { idInstance -> "Collector" }.withNumInstances(1, 1)

    return ts
}


GParsRuntime runtime = new GParsRuntime()
def ts = createMainTasksSet(runtime)
runtime.run(ts)

