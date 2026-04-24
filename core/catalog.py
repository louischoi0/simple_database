CATALOG_PAGE_INDEX = 0

class Object:
  def __init__(self, id, name, type, length, desc):
    self.oid = id
    self.name = name
    self.desc = desc
    self.type = type
    self.length = length

type_type = Object(0, "typeType", None, None, "")
type_page = Object(1, "typePage", type_type, None, "")
type_table = Object(2, "typeTable", type_type, None, "")
type_int = Object(3, "typeInt", type_type, None, "")
type_string = Object(4, "typeString", type_type, None, "")

TYPES = {
  "page": type_page,
  "table": type_table,
  "int": type_int,
  "string": type_string,
}

def get_type(name): 
  return TYPES[name]

obj_page = Object(5, "page", get_type("page"), None, "")
obj_table = Object(6, "table", get_type("table"), None, "")

OBJECTS = {
  "page": obj_page,
  "table": obj_table,
}




