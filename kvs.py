

# Dictionary class to help implemnt KVS
class KVS:

    # Constructor
    def __init__(self):
        self.__dict = {}

    # Method to check if the key exists
    def key_exists(self, key):
        if key in self.__dict:
            return True
        return False

    # Adds or updates a new key, value pair
    def put(self, key, value):
        self.__dict[key] = value
        return
    # Deletes data by key
    def delete(self, key):
        del self.__dict[key]
        return
    # Get value given the key
    def get(self, key):
        return self.__dict[key]

    def get_dictionary(self):
        return self.__dict

    def set_dictionary(self, new_dict):
        self.__dict = new_dict
