Plain Paxos 
===========
Tom Cocagne <tom.cocagne@gmail.com>
v2.0, January 2013


Overview
--------

This repository contains a basic implementation of the Paxos algorithm. The
distinguishing characteristic of this implementation, as compared to other
freely available and open-source implementations, is that this library is
completely independent of application domains and networking
infrastructures. Whereas most Paxos implementations are deeply and inextricably
embedded within application-specific logic, this implementation focuses on
encapsulating the Paxos logic within opaque and easily re-usable classes.

The goal of this implementation is to provide an algorithmically correct Paxos
implementation that may be used for educational purposes in addition to direct
use in networked applications. This implementation is specifically designed
to facilitate understanding of both the essential Paxos algorithm as well as
the practical considerations that must be taken into account by real-world
implementations. The intent is to provide an excellent basis for Paxos 
newcomers to learn about and experiment with distributed consistency. 


Implementation
--------------

### essential.py


This module provides a direct and minimal implementation of the essential Paxos
algorithm. The primary purpose of this module is educational as it allows for
easy contrast between the implementation of the pure algorithm and that of the
one enhanced for practicality.


### practical.py


This module enhances the essential Paxos algorithm and adds support for such
things as leadership tracking, NACKs, and state persistence.


### functional.py


This module provides a fully-functional Paxos implementation that employs
a simple heartbeating mechanism to detect leadership failure and initiate
recovery.


### durable.py


Correct implementations of the Paxos algorithm require saving Acceptor
state to persistent media prior to sending Prepare and Accept messages over the 
network. This is necessary to ensure that promises made to external entities
will never be reneged upon should the application crash and recover at an
inopportune time. This module implements a very simple mechanism for efficiently
saving application state to disk. 


Testing
-------

As this library serves to provide correctness guarantees to higher-level consumers,
this library's testing must be comprehensive and exhaustive. The +test+
directory of the root source code repository contains the unittest files used to
excersise the implementation. 
