import json
from pprint import pprint

class Template():
    """description of class"""

    file_path = None

    def __init__(self, path):
        self.file_path = path     
    
    def get_product_template(self, file_name, item_id, parent_item_id):
        product_template = self.get_template(file_name)
        product_template["itemId"] = item_id
        product_template["parentItemId"] = parent_item_id
        return product_template

    def get_cart_template(self, file_name, cart_id, user_id):
        cart_template = self.get_template(file_name)
        cart_template["cartId"] = cart_id
        cart_template["member"]["userId"] = user_id
        return cart_template

    def get_relationship_template(self):
        raise NotImplementedError("Not imlemented.")

    def get_template(self, filename):
        file = self.file_path + "/" + filename
        with open(file) as data_file:    
             _template = json.load(data_file)

        return _template