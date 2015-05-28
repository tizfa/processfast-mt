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

package it.cnr.isti.hlt.processfast_gpars.utils

public class SoftwareInfo {

    private final String branch;
    private final String fullID;
    private final String shortID;
    private final String IDdescription;
    private final String buildUsername;
    private final String buildUsernameEmail;
    private final String buildTime;
    private final String commitUsername;
    private final String commitUsernameEmail;
    private final String commitMessageShort;
    private final String commitMessageFull;
    private final String commitTime;
    private final String version;
    private final int majorVersion;
    private final int minorVersion;
    private final int subminorVersion;
    private final String revisionVersion;
    private final String changeLog;

    public SoftwareInfo() {
        Properties properties = new Properties();
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(
                    "processfast-gpars.properties");
            properties.load(getClass().getClassLoader().getResourceAsStream(
                    "processfast-gpars.properties"));

            branch = properties.getProperty("processfast-gpars.branch");
            fullID = properties.getProperty("processfast-gpars.commit.id");
            shortID = properties.getProperty("processfast-gpars.commit.id.abbrev");
            IDdescription = properties.getProperty("processfast-gpars.commit.id.describe");
            buildUsername = properties.getProperty("processfast-gpars.build.user.name");
            buildUsernameEmail = properties
                    .getProperty("processfast-gpars.build.user.email");
            buildTime = properties.getProperty("processfast-gpars.build.time");
            commitUsername = properties.getProperty("processfast-gpars.commit.user.name");
            commitUsernameEmail = properties
                    .getProperty("processfast-gpars.commit.user.email");
            commitMessageShort = properties
                    .getProperty("processfast-gpars.commit.message.short");
            commitMessageFull = properties
                    .getProperty("processfast-gpars.commit.message.full");
            commitTime = properties.getProperty("processfast-gpars.commit.time");
            version = properties.getProperty("processfast-gpars.version");
            String[] tags = version.trim().split("[\\.]|[\\-]");
            majorVersion = Integer.parseInt(tags[0]);
            minorVersion = Integer.parseInt(tags[1]);
            subminorVersion = Integer.parseInt(tags[2]);
            if (tags.length < 4) {
                revisionVersion = "";
            } else if (tags.length == 4 && tags[3].equals("SNAPSHOT")) {
                revisionVersion = "";
            } else
                revisionVersion = tags[3];

            is.close();

            is = getClass().getClassLoader().getResourceAsStream(
                    "changelog.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line = reader.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                sb.append(line + "\n");
                line = reader.readLine();
            }
            changeLog = sb.toString();
            reader.close();

        } catch (Exception e) {
            throw new RuntimeException("Reading information about software", e);
        }
    }

    public String getFullID() {
        return fullID;
    }

    public String getShortID() {
        return shortID;
    }

    public String getIDdescription() {
        return IDdescription;
    }

    public String getBuildUsername() {
        return buildUsername;
    }

    public String getBuildUsernameEmail() {
        return buildUsernameEmail;
    }

    public String getBuildTime() {
        return buildTime;
    }

    public String getCommitUsername() {
        return commitUsername;
    }

    public String getCommitUsernameEmail() {
        return commitUsernameEmail;
    }

    public String getCommitMessageShort() {
        return commitMessageShort;
    }

    public String getCommitMessageFull() {
        return commitMessageFull;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public String getVersion() {
        return version;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getSubminorVersion() {
        return subminorVersion;
    }


    public String getRevisionVersion() {
        return revisionVersion;
    }

    public String getBranch() {
        return branch;
    }

    public String getChangeLog() {
        return changeLog;
    }
}

