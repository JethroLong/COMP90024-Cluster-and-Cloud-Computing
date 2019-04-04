import csv
import json
import ijson
from collections import Counter

if __name__ == "__main__":
    with open("/Users/jethrolong/Desktop/smallTwitter.json", 'r') as f:
        # parser = ijson.parse(f,1)
        # for prefix, event, value in parser:
        #     print('prefix={}, event={}, value={}'.format(prefix, event, value))


        # try ijson -- valid
        # records = ijson.items(f, "rows.item.doc")
        # row_count = 0
        # new_records = []
        # for line in records:
        #     row_count += 1
        #     combo = [line["entities"]["hashtags"]] + [line["coordinates"]["coordinates"]]
        #     print(combo,"   ",len(combo))

        # try json line by line
        # record = []
        # f1 = f.readline()
        # f2 = f.readline()
        # f2 = f2[:-2]
        # print(f1)
        # print(f2)
        # print(json.loads(f2))

        # try seek
        row_indicator = 0
        for line in f:
            if not (line.endswith("[\n") or line.endswith("]}\n")):
                row_indicator += 1
                if line.endswith("}},\n"):
                    line = line[:-2]
                else:
                    line = line[:-1]
                print(json.loads(line), row_indicator)

