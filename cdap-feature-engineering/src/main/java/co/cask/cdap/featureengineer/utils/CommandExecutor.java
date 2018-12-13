/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.featureengineer.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author bhupesh.goel
 *
 */
public class CommandExecutor {

    private static class StreamGobbler extends Thread {
        private InputStream is;
        private String output;

        StreamGobbler(InputStream is) {
            this.is = is;
            this.output = "";
        }

        public void run() {
            try {
                StringBuilder sb = new StringBuilder();
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                    sb.append("\n");
                }
                br.close();
                output = sb.toString();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private String commandOutput;
    private String errorOutput;

    public int executeCommand(final String command, final String arg) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command, arg);
        Process process = pb.start();

        // any error message?
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());

        // any output?
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());

        // kick them off
        errorGobbler.start();
        outputGobbler.start();

        int errCode = process.waitFor();
        errorGobbler.join();
        outputGobbler.join();
        commandOutput = outputGobbler.output;
        errorOutput = errorGobbler.output;
        return errCode;
    }

    public String getCommandOutput() {
        return commandOutput;
    }

    public String getErrorOutput() {
        return errorOutput;
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        CommandExecutor executor = new CommandExecutor();
        int errCode = executor.executeCommand("/anaconda2/bin/python", "/var/folders/dn/8y9zxh4n0vv3c4njqjkhqdq9pn3s7z"
                + "/T/temp-feature-engineering-python9215664053409143847.tmp");
        System.out.println("Error code = " + errCode);
        System.out.println("Output = " + executor.commandOutput);
    }
}
