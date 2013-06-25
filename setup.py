#!/usr/bin/env python

from distutils.core import setup

long_description = '''
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
'''

setup(name='essential-paxos',
      version='2.0',
      description='Paxos algorithm implementation suitable for practical and educational use',
      long_description=long_description,
      author='Tom Cocagne',
      author_email='tom.cocagne@gmail.com',
      url='https://github.com/cocagne/paxos',
      packages=['paxos'],
      license='MIT',
      classifiers=['Development Status :: 5 - Production/Stable',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Topic :: System :: Distributed Computing',
                   'Topic :: System :: Networking']
      )
