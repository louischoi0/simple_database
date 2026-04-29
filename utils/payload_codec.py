from utils.buffer_cursor import buffer_cursor
from core.const import *

class payload_codec:

  @classmethod
  def write_dynamic_type_value(self, cursor, value):
      if value is None:
          cursor.write_int64_a(PRIMITIVE_NULL_TYPE_FLAG)

      if isinstance(value, int):
          cursor.write_int64_a(PRIMITIVE_INT_TYPE_FLAG)
          cursor.write_int64_a(value)

      elif isinstance(value, str):
          cursor.write_int64_a(PRIMITIVE_VARCHAR_TYPE_FLAG)
          cursor.write_varchar_a(value)
  
  @classmethod
  def read_dynamic_type_value(self,cursor):
      type_flag = cursor.read_int64()

      if type_flag == PRIMITIVE_NULL_TYPE_FLAG:
          return None
      elif type_flag == PRIMITIVE_INT_TYPE_FLAG:
          return cursor.read_int64()
      elif type_flag == PRIMITIVE_VARCHAR_TYPE_FLAG:
          return cursor.read_varchar()

  @classmethod
  def write_key_header(cls, cursor, keyname):
      cursor.write_varchar_a(keyname)
      cursor.write_varchar_a("=")

  @classmethod
  def read_key_header(cls, cursor):
      keyname = cursor.read_varchar()
      eq = cursor.read_varchar()
      assert eq == "="
      return keyname
  
  @classmethod
  def encode(cls, dictionary):
      cursor = buffer_cursor()

      for k in dictionary:
          v = dictionary[k]
          cls.write_key_header(cursor, k)
          cls.write_dynamic_type_value(cursor, v)

      return cursor.buffer

  @classmethod
  def decode(cls, buffer):
      cursor = buffer_cursor(buffer)
      res = {}
      c = 0
      while c < 2:
          key_name = cls.read_key_header(cursor)
          value = cls.read_dynamic_type_value(cursor)
          res[key_name] = value
          c += 1
      
      return res

