import sys

def convert_department_code(code):
  if code == "2A":
    return 265  # Code ASCII de 'A' est 65
  elif code == "2B":
    return 266  # Code ASCII de 'B' est 66
  try:
    return int(code)
  except ValueError:
    return code


sys.modules[__name__] = convert_department_code
