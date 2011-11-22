#!/usr/bin/env python

"""
Unit tests for the EsuRestApi class
"""

import unittest, random, string
from EsuRestApi import EsuRestApi

class EsuRestApiTest(unittest.TestCase):
    
    # Enter your own host in the form of sub.domain.com or 10.0.1.250
    host = " "
    
    # Enter the port where Atmos lives here
    port = 80
    
    # Enter your full UID in the form of something/something_else
    uid = " "
    
    # Enter your secret here.  (shhsh!)
    secret = " "
    
    oid_clean_up = []
    path_clean_up = []
    
    def setUp(self):
        print "\n Starting setup"     
        self.esu = EsuRestApi(self.host, self.port, self.uid, self.secret)
        
    def tearDown(self):
        print "\n Tearing down"
        
        if self.oid_clean_up:
            for object in self.oid_clean_up:
                self.esu.delete_object(object)
        
        if self.path_clean_up:
            dir = self.path_clean_up[0].split("/")
            self.esu.delete_directory(dir[0])

    def test_create_empty_object(self):
        data = " "
        oid = self.esu.create_object(data=data)
        self.assertTrue(oid, "null object ID returned")
        object = self.esu.read_object(oid)
        self.assertEqual(object, data, "wrong object content")
        self.oid_clean_up.append(oid)
        
    def test_create_empty_object_on_path(self):
        data = " "
        path = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8)) + "/file.data"
        oid = self.esu.create_object_on_path(data=data, path=path)
        self.assertTrue(oid, "null object ID returned")
        object = self.esu.read_object(oid)
        self.assertEqual(object, data, "wrong object content")
        self.oid_clean_up.append(oid)
        self.path_clean_up.append(path)
            
    def test_create_object_with_content(self):
        data = "The quick brown fox jumps over the lazy dog"
        oid = self.esu.create_object(data=data)
        object = self.esu.read_object(oid)
        self.assertEquals(data, object)
        self.oid_clean_up.append(oid)
    
    def test_create_object_on_path_with_content(self):
        data = "The quick brown fox jumps over the lazy dog"
        path = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8)) + "/file.data"
        oid = self.esu.create_object_on_path(data=data, path=path)
        self.assertTrue(oid, "null object ID returned")
        object = self.esu.read_object(oid)
        self.assertEqual(object, data, "wrong object content")
        self.oid_clean_up.append(oid)
        self.path_clean_up.append(path)
    
    def test_create_object_on_path_with_metadata(self):
        data = "The quick brown fox jumps over the lazy dog"
        listable_meta = {"key1" : "value1", "key2" : "value2", "key3" : "value3"}
        path = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8)) + "/file.data"
        oid = self.esu.create_object_on_path(data=data, path=path, listable_meta=listable_meta)
        self.assertTrue(oid, "null object ID returned")
        object = self.esu.read_object(oid)
        self.assertEqual(object, data, "wrong object content")
        
        # Retrieves existing metadata for an object and compares it to the known metadata dictionary that was stored
        metadata = self.esu.get_user_metadata(oid)['listable_user_meta']
        self.assertEqual(listable_meta, metadata, "metadata key/values are wrong")
        
        self.oid_clean_up.append(oid)
        self.path_clean_up.append(path)
    
    def test_create_object_with_metadata(self):
        data = "The quick brown fox jumps over the lazy dog"
        listable_meta = {"key1" : "value1", "key2" : "value2", "key3" : "value3"}
        oid = self.esu.create_object(data=data, listable_meta=listable_meta)
        self.assertTrue(oid, "null object ID returned")
        object = self.esu.read_object(oid)
        self.assertEqual(object, data, "wrong object content")
        
        # Retrieves existing metadata for an object and compares it to the known metadata dictionary that was stored
        metadata = self.esu.get_user_metadata(oid)['listable_user_meta']
        self.assertEqual(listable_meta, metadata, "metadata key/values are wrong")
        
        self.oid_clean_up.append(oid)

    def test_read_acl(self):
        data = "The quick brown fox jumps over the lazy dog"
        oid = self.esu.create_object(data=data)
        uid = self.esu.uid.split("/")[0]
        user_acl = "%s=FULL_CONTROL" % uid
        resp = self.esu.set_acl(oid, user_acl)
        
        acl = self.esu.get_acl(oid)['user_acl'][uid]
        #print user_acl
        self.assertEqual(acl, "FULL_CONTROL", "acl does not match")
        
        self.oid_clean_up.append(oid)

    
    def test_delete_user_metadata(self):
        pass
    
    def test_get_system_metadata(self):
        pass
    
    def test_list_objects(self):
        pass
    
    
    

if __name__ == "__main__":
    test_classes = [ EsuRestApiTest ]
    for test_class in test_classes:
        temp = str(test_class)
        name = temp.split('.')[-1][:-2]
        print "Start of test for", name
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        unittest.TextTestRunner(verbosity=2).run(suite)
        print "End of test for", name
    