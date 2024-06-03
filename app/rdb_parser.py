import codecs
from pathlib import Path


class RdbParser:
    def __init__(self, file: Path):
        self.db_file = file
        self.kv = {}

    def _read_data(self) -> bytes:
        if self.db_file.is_file():
            f = open(self.db_file, "rb")
            return f.read()
        else:
            print("File doesn't exist")
            return b""

    def _get_content(self, data: bytes) -> list:
        if data == b"":
            return None
        res = data.hex(" ")
        print(f"[Hex data] {res}")
        idxofb = res.index("fb")
        idxoff = res.index("ff")
        d = res[idxofb:idxoff].split(" ")
        print("[d]", d)
        # lst = res[idxofb + 1 : idxoff].split("00")
        # reslst = []
        # for x in lst:
        #     reslst.append(str(x).strip().split(" "))
        # print("[lst]", lst)
        # print("[reslst]", reslst)
        # hash_table_size = int(reslst[0], 16)
        # exp_hash_table_size = int(reslst[1], 16)
        # exp_table: list = []
        # while len(exp_table) < exp_hash_table_size:
        #     exp_table.append()
        # return reslst

    def _extract_key_value_pairs(self, data: list):
        if data == None:
            return None
        data.pop(0)
        result = {}
        for x in data:
            if len(x) == 0:
                continue
            lengthKey = int(x[0])
            l1 = x[1 : lengthKey + 1]
            l2 = x[lengthKey + 1 :]
            key = ""
            value = ""
            value_integer = False
            for byt in l1:
                key += byt
            if "c0" in l2[0]:
                value_integer = True
                value = int(l2[1], 16)
                break
            else:
                l2.pop(0)
                for byt in l2:
                    value += byt
            key = codecs.decode(key, "hex").decode("utf-8")
            if not value_integer:
                value = codecs.decode(value, "hex").decode("utf-8")
            result[f"{key}"] = f"{value}"
        return result

    def get_keys(self) -> list:
        data = self._read_data()
        print("Data read:", data)
        if data == b"":
            return []
        print("the data is read")
        trimmedData = self._get_content(data)
        print("Trimmed data", trimmedData)
        self.kv = self._extract_key_value_pairs(trimmedData)
        print("key recieved", self.kv)
        result = []
        for k in self.kv:
            result.append(k)
        return result


# p = RdbParser(Path("./file.bin"))
# p.get_keys()
