import unittest
import data_store
ds = data_store.data_store()



class TestCalc(unittest.TestCase):

    def test_Key_type(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckKey(None)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckKey(3)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckKey(1.2)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckKey(True)
        
    def test_Value_type(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckValue(None)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckValue(3)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckValue(1.2)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckValue(True)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.CheckValue('string')

    def test_Value_size(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            limit = 1024 * 2 # 1024*2*8 +10 > 16KB
            value = '{'
            for i in range(limit):
                value = value + '"Age' + str(i) + '":2,'
            value = value + '"Roll": 3}'
            ds.Create('key', value)
        
        try:
            ds.ClearAll()
            limit = 1024 # 1024*8 =  8KB
            value = '{'
            for i in range(limit):
                value = value + '"Age' + str(i) + '":2,'
            value = value + '"Roll": 3}'
            ds.Create('key', value)
        except :
            self.fail()
        
    def test_Key_size(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            key = 'a'*33
            ds.Create(key, '{"Age":2}')
        
        try:
            ds.ClearAll()
            key = 'a'*32
            ds.Create(key, '{"Age":2}')
        except :
            self.fail()
        
    def test_ttl_type(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', True)
        
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', 'string')
        
    def test_ttl_value(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', -3)

    def test_Create_type(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(None)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create({'a':1})
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(None, 3)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(None, 1.2)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(None, True)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(3)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(1.2)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create(True)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', 3)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', 1.2)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', True)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', 'string')
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', False)
        with self.assertRaises(Exception):
            ds.ClearAll()
            ds.Create('key', '{"Age":2}', -3)

    def test_Create_size(self):
        with self.assertRaises(Exception):
            ds.ClearAll()
            limit = 1024 * 2 # 1024*2*8 + 10 > 16KB
            value = '{'
            for i in range(limit):
                value = value + '"Age' + str(i) + '":2,'
            value = value + '"Roll": 3}'
            ds.Create('key', value)
        
        try:
            ds.ClearAll()
            limit = 1024 # 1024*8 =  8KB
            value = '{'
            for i in range(limit):
                value = value + '"Age' + str(i) + '":2,'
            value = value + '"Roll": 3}'
            ds.Create('key', value)
        except :
            self.fail()
        
        with self.assertRaises(Exception):
            ds.ClearAll()
            key = 'a'*33
            ds.Create(key, '{"Age":2, "Roll":3}')
        
        try:
            ds.ClearAll()
            key = 'a'*32
            ds.Create(key, '{"Age":2, "Roll":3}')
        except :
            self.fail()
        
    
    def test_Read(self):
        with self.assertRaises(Exception):
            ds.Read(None)
        with self.assertRaises(Exception):
            ds.Read(3)
        with self.assertRaises(Exception):
            ds.Read(True)
        with self.assertRaises(Exception):
            ds.Read({'a':1})
        try:
            ds.ClearAll()
            ds.Create('key', '{"Age":2, "Roll":3}')
            ds.Read('key')
        except :
            self.fail()

    def test_Delete(self):
        with self.assertRaises(Exception):
            ds.Delete(None)
        with self.assertRaises(Exception):
            ds.Delete(3)
        with self.assertRaises(Exception):
            ds.Delete(True)
        with self.assertRaises(Exception):
            ds.Delete({'a':1})
        
        
        

if __name__ == '__main__':
    unittest.main()
    ds.ClearAll()