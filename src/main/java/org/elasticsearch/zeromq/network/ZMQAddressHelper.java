/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.zeromq.network;

/**
 * @author tlrx
 */
public class ZMQAddressHelper {

    private class Address {
        private String type = null;
        private String hostname = null;
        private int port = 0;

        public Address() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    private Address resolve(String address) {

        if (address == null) {
            return null;
        }
        Address ad = new Address();
        String[] splits = address.split(":");

        if (splits == null || splits.length <= 1) {
            return null;

        } else {
            ad.setType(splits[0]);

            if ("tpc".equals(splits[0])) {

                String host = splits[1].replaceFirst("//", "");
                if ("*".equals(host)) {
                    ad.setHostname("localhost");
                } else {
                    ad.setHostname(host);
                }
            }

            if (splits.length >= 3) {
                int port = 0;
                try {
                    port = Integer.parseInt(splits[2]);
                } catch (NumberFormatException e) {
                    // Apply some default value. Do not let the "catch" block empty !
                    port = 0;
                }
                ad.setPort(port);
            }
        }
        return ad;
    }

    public static String getHostName(String address) {
        Address ad = new ZMQAddressHelper().resolve(address);

        if (ad != null) {
            return ad.getHostname();
        } else {
            return null;
        }

    }

    public static int getPort(String address) {
        Address ad = new ZMQAddressHelper().resolve(address);

        if (ad != null) {
            return ad.getPort();
        } else {
            return 0;
        }

    }

}
