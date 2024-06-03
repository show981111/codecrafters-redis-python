def parse_redis_file_format(data: str):
    splited_parts = data.split("\\")
    resizedb_index = splited_parts.index("xfb")
    key_index = resizedb_index + 4
    value_index = key_index + 1
    key_bytes = splited_parts[key_index]
    key = remove_bytes_caracteres(key_bytes)
    return key


def remove_bytes_caracteres(string: str):
    if string.startswith("x"):
        return string[3:]
    elif string.startswith("t"):
        return string[1:]
