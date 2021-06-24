Essential Paxos 
===============
Tom Cocagne &lt;tom.cocagne@gmail.com&gt;  
v2.0, January 2013


Introductory Note
-----------------

This repository contains the results of my first foray into providing both a
useful and educational Paxos implementation. The repository is in pretty good
shape and still fills its intended role but the design includes explicit
support for sending network messages. I now consider this something of an
anti-pattern for Paxos implementations and recommend using an even more tightly
constrained library that implements the core algorithm and nothing else,
including messaging. To that end, I've put together the
[python-composable-paxos](https://github.com/cocagne/python-composable-paxos)
repository and a
[multi-paxos-example](https://github.com/cocagne/multi-paxos-example) based upon
it. Those repositories should generally be preferred over this one.
Additionally, I've written an [introductory paper on Paxos and
Multi-Paxos](https://understandingpaxos.wordpress.com/) that may be useful for
individuals like myself that find the existing papers somewhat opaque.


Overview
--------

Essential Paxos provides basic implementations of the Paxos algorithm. The
distinguishing characteristic of this implementation, as compared to other
freely available and open-source implementations, is that this library is
independent of application domains and networking infrastructures. Whereas most
Paxos implementations are deeply and inextricably embedded within
application-specific logic, this implementation focuses on encapsulating the
Paxos algorithm within opaque and easily re-usable classes.

This library provides an algorithmically correct Paxos implementation that may
be used for educational purposes in addition to direct use in networked
applications. This implementation is specifically designed to facilitate
understanding of both the essential Paxos algorithm as well as the practical
considerations that must be taken into account for real-world use.

Implementations in both Python and Java are provided. 


Python Installation
-------------------

```bash
$ easy_install essential-paxos
```


Implementations
---------------

### Python


#### essential.py


This module provides a direct and minimal implementation of the essential Paxos
algorithm. The primary purpose of this module is educational as it allows for
easy contrast between the implementation of the pure algorithm and that of the
one enhanced for practicality.


#### practical.py


This module enhances the essential Paxos algorithm and adds support for such
things as leadership tracking, NACKs, and state persistence.


#### functional.py


This module provides a fully-functional Paxos implementation that employs
a simple heartbeating mechanism to detect leadership failure and initiate
recovery.


#### external.py

This module provides an enhanced version of practical.py that supports
the use of external failure detectors to drive leadership management. This module
does not provide a fully-functional solution to leadership management, as does
functional.py. However, it may serve as the basis for much more flexible, 
application-specific leadership management.


#### durable.py


Correct implementations of the Paxos algorithm require saving Acceptor
state to persistent media prior to sending Promise and Accepted messages over the 
network. This is necessary to ensure that promises made to external entities
will never be reneged upon should the application crash and recover at an
inopportune time. This module implements a very simple mechanism for efficiently
saving application state to disk. 


### Java

The Java implementation is functionally identical to that of the Python
implementation and is broken out into Java packages that mirror the 
python modules. Refer to the corresponding Python APIs for class and
method documentation.

#### cocagne.paxos.essential
#### cocagne.paxos.practical
#### cocagne.paxos.functional



Testing
-------

The `test` directory of the root source code repository contains the unittest
files used to exercise the implementation. The *primary* tests are written in
Python and correspond to the essential, practical, functional, and durable
modules. The Java tests, which are also written in Python, wrap the Java classes
with a compatible interface and use the Python unit tests to exercise the Java
implementation. The Jython interpreter is required for running these tests but
it is not required at runtime.
