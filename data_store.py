import json, os, threading, sys, time
from filelock import FileLock

# File lock and data_lock for data_store
file_lock = FileLock("data_store.json.lock")
data_lock = threading.Lock()


class data_store:
    def __init__(self, file_path = os.getcwd(), file_max_size = pow(2, 30), key_max_len = 32, value_max_size = 16*pow(2,10)):

        # creating a filepath for data store
        self.file_path = file_path + "/data_store.json"
        print(self.file_path)
        # Defining max limits
        # 1GB = 2^30 Byte, 16KB = 16*(2^10) Byte
        self.FILE_MAX_SIZE = file_max_size
        self.KEY_MAX_LEN = key_max_len
        self.VALUE_MAX_SIZE = value_max_size

        # Acquiring data_lock and file_lock to acess data, if it's there
        file_lock.acquire()
        data_lock.acquire()

        try:
            # if there exist a file, store the data
            file = open(self.file_path, 'r')
            if(os.path.getsize(self.file_path) == 0):
                file.write("{}")
            print("Data Store File Found in Directory")

        except:
            # No file Found, Create a new file
            file = open(self.file_path, 'w')
            file.write("{}")
            print("Data Store File Created in Directory")

        finally:
            # Closing the file
            file.close()

            # Releasing both locks
            data_lock.release()
            file_lock.release()


    def CheckKey(self, key):

        # Check if key meets the following parameters
        # Check if it's a string
        if(type(key) == str):

            # Check if it's empty
            if key == '':

                # Raise Exception, as key can not be empty
                raise Exception('No key was provided.')

            # Check if key size has greater than KEY_MAX_SIZE
            if(len(key) > self.KEY_MAX_LEN):
                # If condition didn't met, raise exception
                raise Exception('Key length should be less or equal to '  + self.KEY_MAX_LEN) +". "+ 'Length of Key is ' + str(len(key))
            
        # doesn't satisfies all conditions
        else: raise Exception('Key type should be string, Type of key is ' + str(type(key)))
        

    def CheckValue(self, value):

        # Try for
        try:
            json_object = json.loads(value)

        except:
            raise Exception('Value type should be a JSON object, Type of value is ' + str(type(value)))
        
        # Check if value has greater than 32 char
        if(sys.getsizeof(value) > self.VALUE_MAX_SIZE):

            # does'nt satisfies size conditions
            raise Exception('Size of value should be less or equal to '  + str(self.VALUE_MAX_SIZE) +'B. Size of Value is ' + str(self.VALUE_MAX_SIZE) + 'B.')

        
    def Checkttl(self, ttl):
        
        # only integer or float will allow addition
        if not (type(ttl) == int or type(ttl) == float):
            raise Exception("Time-To-Live should be an integer or float, currently it's " + str(type(ttl)))

        # Time-To-Live can't be negative
        if(ttl<0):
            raise Exception("Time-To-Live Can't be negative")
        

    def CheckSize(self, object_size = 0):

        # Acquiring the Lock and getting existing size and releasing
        file_lock.acquire()
        file_size = os.path.getsize(self.file_path)
        file_lock.release()

        # Checks if the size of the value exceeds 16 KB.
        if object_size > self.VALUE_MAX_SIZE:
            raise Exception("Size of the value exceeds " + str(self.VALUE_MAX_SIZE) +" size limit.")

        # if the total size is greater than FILE_MAX_SIZE then, raise an exception
        if (file_size + object_size) > self.FILE_MAX_SIZE:
            raise Exception("The size of File is: " + str(object_size) + ". Can't be accomodated in " + str(file_size) + ".")
    


    def Create(self, key = "", value = None, time_to_live = 100):
        
        # Check for invalid key aur value
        self.CheckKey(key)
        self.CheckValue(value)
        self.Checkttl(time_to_live)
        
        # Checking Size of object
        self.CheckSize(sys.getsizeof(value) + sys.getsizeof(key))

        # Acquiring Data Lock for Checking and adding
        file_lock.acquire()
        data_lock.acquire()
        
        # Storing the file data
        data = {}
        with open('data_store.json') as json_file:
            # print(json_file['Nischal'])
            data = json.load(json_file)
            # print(y)
            # data = json.load(json_file)
    

        try:
            # Check if it is already present in data
            if key in data.keys() and data[key]['ttl'] < time.time():

                # Raising the Exception
                raise Exception('Key is already present.')

            # time_to_live would be store till when the data would be accessible
            time_to_live = time.time() + time_to_live
            
            # Storing it as a combined value, would store both the 
            combined_value = {'value': value, 'ttl': time_to_live}
            data[key] = combined_value

            # Storing the key-value in JSON file
            with open('data_store.json', 'w') as file:
                json.dump(data, file)

        finally:
            # Releasing both locks
            data_lock.release()
            file_lock.release()

        print('Entry added for ' + str(key))


    def Read(self, key = None):

        # Check if key satisfies parameters
        self.CheckKey(key)

        # Acquiring data_lock and file_lock to acess data
        file_lock.acquire()
        data_lock.acquire()

        data = {}
        with open('data_store.json', 'r') as file:
            data = json.load(file)

        # Searching for the given key
        if key in data.keys():

            # If the entry id live
            if data[key]['ttl'] >= time.time():

                # Printing the key and value
                print(key + ": " + json.dumps(data[key]['value']))
                # Releasing both locks
                data_lock.release()
                file_lock.release()

            else:
                # Releasing both locks
                file_lock.release()
                data_lock.release()
                # Raising Exception as the key can't be accessed as it's not live
                raise Exception("Key's Time-To-Live has expired, not Available for Read operation")
            
        else:
            # Releasing both locks
            file_lock.release()
            data_lock.release()

            # Raising the Exception
            raise Exception('Key is not present.')
        


    def Delete(self, key = None):
        
        # Check key for all the parameters
        self.CheckKey(key)

        # Acquiring data_lock and file_lock to acess data
        file_lock.acquire()
        data_lock.acquire()

        data = {}
        with open('data_store.json', 'r') as file:
            data = json.load(file)

        # Searching for the given key
        if key in data.keys():

            # If the entry id live
            if data[key]['ttl'] >= time.time():

                # Printing the key and value
                del data[key]

                # Storing the key-value in JSON file
                with open('data_store.json', 'w') as file:
                    json.dump(data, file)
                print("Key Deleted.")
                # Releasing both locks
                data_lock.release()
                file_lock.release()

            else:
                # Releasing both locks
                data_lock.release()
                file_lock.release()
                raise Exception("Key's Time-To-Live has expired, not available for Delete operation")
            
        else:
            # Releasing both locks
            data_lock.release()
            file_lock.release()
            raise Exception('Key is not present.')

    
    def DisplayAll(self):

        # Acquiring data_lock and file_lock to acess data
        file_lock.acquire()
        data_lock.acquire()

        data = {}
        with open('data_store.json', 'r') as file:
            data = json.load(file)

        # Releasing both locks
        data_lock.release()
        file_lock.release()

        # printing the data of given file
        print(data)
    
    def ClearAll(self):

        # Acquiring data_lock and file_lock to acess data
        file_lock.acquire()
        data_lock.acquire()

        data = {}
        # Storing the key-value in JSON file
        with open('data_store.json', 'w') as file:
            json.dump(data, file)

        # Releasing both locks
        data_lock.release()
        file_lock.release()
        
        # printing the data of given file
        # print('Cleared All')
        
# Examples
# ds = data_store()
# v = '{"AGE":21, "ROLL": 11}'
# try:
#     threading.Thread(ds.Create('Jim', v)).start()
# except Exception as e:
#     print(e)
# try:
#     threading.Thread(ds.Read('Jim')).start()
# except Exception as e:
#     print(e)
# try:
#     threading.Thread(ds.Delete('Jim')).start()
# except Exception as e:
#     print(e)

