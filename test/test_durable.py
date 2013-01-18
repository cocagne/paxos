
import sys
import os
import os.path
import hashlib
import struct
import tempfile
import shutil
import pickle

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import durable


class DObj(object):

    def __init__(self):
        self.state = 'initial'
        

        
class DurableReadTester (unittest.TestCase):

    
    def setUp(self):
        tmpfs_dir = '/dev/shm' if os.path.exists('/dev/shm') else None
        self.tdir   = tempfile.mkdtemp(dir=tmpfs_dir)
        self.fds    = list()

        
    def tearDown(self):
        shutil.rmtree(self.tdir)
        for fd in self.fds:
            os.close(fd)


    def newfd(self, data=None):
        bin_flag = 0 if not hasattr(os, 'O_BINARY') else os.O_BINARY
        
        fd = os.open( os.path.join(self.tdir, str(len(self.fds))),  os.O_CREAT | os.O_RDWR | bin_flag)

        self.fds.append( fd )

        if data is not None:
            os.write(fd, data)

        return fd
        
        
    def test_read_zero_length(self):
        self.assertRaises(durable.FileTruncated, durable.read, self.newfd())

    def test_read_header_too_small(self):
        self.assertRaises(durable.FileTruncated, durable.read, self.newfd('\0'*31))

    def test_read_no_pickle_data(self):
        data = '\0'*24 + struct.pack('>Q', 5)
        self.assertRaises(durable.FileTruncated, durable.read, self.newfd(data))

    def test_read_bad_hash_mismatch(self):
        data = '\0'*24 + struct.pack('>Q', 5) + 'x'*5
        self.assertRaises(durable.HashMismatch, durable.read, self.newfd(data))

    def test_read_ok(self):
        pdata = 'x'*5
        p     = pickle.dumps(pdata, pickle.HIGHEST_PROTOCOL)
        data  = '\0'*8 + struct.pack('>Q', len(p)) + p
        data  = hashlib.md5(data).digest() + data
        self.assertEqual( durable.read(self.newfd(data) ), (0, pdata) )
        
        

class DurableObjectHandlerTester (unittest.TestCase):

    
    def setUp(self):
        tmpfs_dir = '/dev/shm' if os.path.exists('/dev/shm') else None
        self.o      = DObj()
        self.tdir   = tempfile.mkdtemp(dir=tmpfs_dir)
        self.doh    = durable.DurableObjectHandler(self.tdir, 'id1')

        self.dohs   = [self.doh,]

        
    def tearDown(self):
        for doh in self.dohs:
            doh.close()
        shutil.rmtree(self.tdir)
        

    def newdoh(self, obj_id=None):
        if obj_id is None:
            obj_id = 'id' + str(len(self.dohs))
        doh = durable.DurableObjectHandler(self.tdir, obj_id)
        self.dohs.append(doh)
        return doh


    def test_bad_directory(self):
        self.assertRaises(Exception, durable.DurableObjectHandler, '/@#$!$^FOOBARBAZ', 'blah')
        

    def test_no_save(self):
        self.doh.close()
        d = self.newdoh('id1')
        self.assertEquals(d.recovered, None)
        self.assertEquals(d.serial, 1)

    def test_one_save(self):
        self.doh.save(self.o)
        self.doh.close()
        d = self.newdoh('id1')
        self.assertTrue( os.stat(self.doh.fn_a).st_size > 0 )
        self.assertTrue( os.stat(self.doh.fn_b).st_size == 0 )
        self.assertTrue( isinstance(d.recovered, DObj) )
        self.assertEquals(d.recovered.state, 'initial')


    def test_two_save(self):
        self.doh.save(self.o)
        self.o.state = 'second'
        self.doh.save(self.o)
        self.doh.close()
        d = self.newdoh('id1')
        self.assertTrue( os.stat(self.doh.fn_a).st_size > 0 )
        self.assertTrue( os.stat(self.doh.fn_b).st_size > 0 )
        self.assertTrue( isinstance(d.recovered, DObj) )
        self.assertEquals(d.recovered.state, 'second')

    def test_three_save(self):
        self.doh.save(self.o)
        self.o.state = 'second'
        self.doh.save(self.o)
        self.o.state = 'third'
        self.doh.save(self.o)
        self.doh.close()
        d = self.newdoh('id1')
        self.assertTrue( isinstance(d.recovered, DObj) )
        self.assertEquals(d.recovered.state, 'third')

        
    def test_new_object_corrupted(self):
        self.test_two_save()

        with open(self.doh.fn_b, 'wb') as f:
            f.write('\0')
            f.flush()
            
        d = self.newdoh('id1')
        self.assertTrue( isinstance(d.recovered, DObj) )
        self.assertEquals(d.recovered.state, 'initial')

        
    def test_old_object_corrupted(self):
        self.test_two_save()

        with open(self.doh.fn_a, 'wb') as f:
            f.write('\0')
            f.flush()
            
        d = self.newdoh('id1')
        self.assertTrue( isinstance(d.recovered, DObj) )
        self.assertEquals(d.recovered.state, 'second')


    def test_unrecoverable_corruption(self):
        self.test_two_save()

        with open(self.doh.fn_a, 'wb') as f:
            f.write('\0')
            f.flush()

        with open(self.doh.fn_b, 'wb') as f:
            f.write('\0')
            f.flush()

        def diehorribly():
            self.newdoh('id1')

        self.assertRaises(durable.UnrecoverableFailure, diehorribly)
