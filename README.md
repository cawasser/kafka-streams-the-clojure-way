# kafka-streams-the-clojure-way

A Clojure library designed to ... well, that part is up to you.

Based upon [this article](https://medium.com/funding-circle/kafka-streams-the-clojure-way-d62f6cefaba1)
## Usage

First run:

On Linux
    docker run --rm --net=host landoop/fast-data-dev

On Mac
    docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost landoop/fast-data-dev:latest


Then connect to an nREPL using "Run with Leiningen"

You can look at the Kafka Console by opening a browser at:

    http://localhost:3030/kafka-topics-ui/#/cluster/fast-data-dev/topic/n/purchase-made/

## License

Copyright © 2019 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
