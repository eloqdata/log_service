# Log Service

Log Service of EloqDB — Store redo log as a separate service.

## Overview

The Log Service is a core component of [EloqDB](https://github.com/eloqdata), a modular database designed for high-performance, transaction and scalability. Log Service is not tied to a specific database engine but serves as a general redo log service. By integrating with various database runtimes and storage engines, such as [EloqSQL](https://github.com/monographdb/eloqsql), it enables fine-grained transaction support.
