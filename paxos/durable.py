'''
This module implements a very simple mechanism for crash-proof storage
of an object's internal state.

The DurableObjectHandler class is given an object to durably persist along with
an object_id that is used to distinguish between multiple durable objects
persisted to the same directory. Whenever a savepoint is reached in the
application, the handler's save() method may be used to store the objects state
to disk. The application or machine is permitted to fail at any point. If
the failure occurs mid-write, the corruption will be detected by the
DurableObjectHandler constructor and the previously stored state will be
loaded.

This implementation does not reliably protect against on-disk corruption.  That
is to say, if the most-recently saved file is modified after it is successfully
written to the disk, such as through hardware or software errors, the saved
state will be permanently lost. As this implementation provides now way to
distinguish between files that failed to write and files that were
corrupted-on-disk, the implementation will assume the initial write failed and
will return the previously saved state (assuming it isn't similarly
corrupted). Consequently, there is a small but real chance that an application
may save its state with this implementation, make promises to external
entities, crash, and reneg on those promises after recovery. The likelihood is,
of course, very very low so this implementation should be suitable for most
"good" reliability systems. Just don't implement a life-support system based on
this code...

Design Approach:

* Toggle writes between two different files
* Include a monotonically incrementing serial number in each write to
  allow determination of the most recent version
* md5sum the entire content of the data to be written and prefix the write
  with the digest
* fsync() after each write

'''

import os
import os.path
import hashlib
import struct

try:
    import cPickle as pickle
except ImportError:
    import pickle

# This module is primarily concerned with flushing data to the disk. The 
# default os.fsync goes a bit beyond that so os.fdatasync is used instead
# if it's available. Failing that, fcntl.fcntl(fd, fcntl.F_FULLSYNC) is
# attempted (OSX equivalent). Failing that, os.fsync is used (Windows).
#
_fsync = None

if hasattr(os, 'fdatasync'):
    _fsync = os.fdatasync

if _fsync is None:
    try:
        import fcntl
        if hasattr(fcntl, 'F_FULLFSYNC'):
            _fsync = lambda fd : fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    except (ImportError, AttributeError):
        pass

if _fsync is None:
    _fsync = os.fsync


# File format
#
#  0:  md5sum of file content
# 16:  serial_number
# 24:  pickle_length
# 32+: pickle_data

class DurabilityFailure (Exception):
    pass

class UnrecoverableFailure (DurabilityFailure):
    pass

class FileCorrupted (DurabilityFailure):
    pass

class HashMismatch (FileCorrupted):
    pass

class FileTruncated (FileCorrupted):
    pass



def read( fd ):
    '''
    Returns: (serial_number, unpickled_object) or raises a FileCorrupted exception
    '''
    os.lseek(fd, 0, os.SEEK_SET)
        
    md5hash       = os.read(fd, 16)
    data1         = os.read(fd, 8)
    data2         = os.read(fd, 8)
    
    if ( (not md5hash or len(md5hash) != 16) or
         (not data1   or len(data1)   !=  8) or
         (not data2   or len(data2)   !=  8) ):
        raise FileTruncated()
    
    serial_number = struct.unpack('>Q', data1)[0]
    pickle_length = struct.unpack('>Q', data2)[0]

    data3         = os.read(fd, pickle_length)

    if not data3 or len(data3) != pickle_length:
        raise FileTruncated()

    m = hashlib.md5()
    m.update( data1 )
    m.update( data2 )
    m.update( data3 )
    
    if not m.digest() == md5hash:
        raise HashMismatch()
    
    return serial_number, pickle.loads(data3)
    

    
def write( fd, serial_number, pyobject ):
    os.lseek(fd, 0, os.SEEK_SET)

    data_pickle = pickle.dumps(pyobject, pickle.HIGHEST_PROTOCOL)
    data_serial = struct.pack('>Q', serial_number)
    data_length = struct.pack('>Q', len(data_pickle))

    m = hashlib.md5()
    m.update( data_serial )
    m.update( data_length )
    m.update( data_pickle )

    os.write(fd, ''.join([m.digest(), data_serial, data_length, data_pickle]))

    _fsync(fd)


class DurableObjectHandler (object):
    
    def __init__(self, dirname, object_id):
        '''
        Throws UnrecoverableFailure if both files are corrupted
        '''
        
        if not os.path.isdir(dirname):
            raise Exception('Invalid directory: ' + dirname)

        sid = str(object_id)

        self.fn_a = os.path.join(dirname, sid + '_a.durable')
        self.fn_b = os.path.join(dirname, sid + '_b.durable')

        sync_dir = False
        
        if not os.path.exists(self.fn_a) or not os.path.exists(self.fn_b):
            sync_dir = True

        bin_flag = 0 if not hasattr(os, 'O_BINARY') else os.O_BINARY
        
        self.fd_a = os.open(self.fn_a, os.O_CREAT | os.O_RDWR | bin_flag)
        self.fd_b = os.open(self.fn_b, os.O_CREAT | os.O_RDWR | bin_flag)

        if sync_dir and hasattr(os, 'O_DIRECTORY'):
            fdd = os.open(dirname, os.O_DIRECTORY | os.O_RDONLY)
            os.fsync(fdd)
            os.close(fdd)

        self.recover()


    def recover(self):

        sa, sb, obja, objb = (None, None, None, None)
            
        try:
            sa, obja = read(self.fd_a)
        except FileCorrupted:
            pass

        try:
            sb, objb = read(self.fd_b)
        except FileCorrupted:
            pass

        if sa is not None and sb is not None:
            s, obj, fd = (sa, obja, self.fd_b) if sa > sb else (sb, objb, self.fd_a)
        else:
            s, obj, fd = (sa, obja, self.fd_b) if sa is not None else (sb, objb, self.fd_a)
                
        if s is None:
            if os.stat(self.fn_a).st_size == 0 and os.stat(self.fn_b).st_size == 0:
                self.serial    = 1
                self.fd_next   = self.fd_a
                self.recovered = None
            else:
                raise UnrecoverableFailure('Unrecoverable Durability failure')
            
        else:
            self.serial    = s + 1
            self.fd_next   = fd
            self.recovered = obj

        return self.recovered


    def close(self):
        if self.fd_a is not None:
            os.close(self.fd_a)
            os.close(self.fd_b)
            self.fd_a = None
            self.fd_b = None

            
    def save(self, obj):
        serial = self.serial
        fd     = self.fd_next
        
        self.serial += 1
        self.fd_next = self.fd_a if self.fd_next == self.fd_b else self.fd_b
        self.recovered = None
    
        write( fd, serial, obj )

        
