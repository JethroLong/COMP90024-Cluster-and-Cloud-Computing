import csv
import json
import re
import ijson
from collections import Counter

HASHTAG_REGEX = "\s#\S+\s"

def find_hashtags(tweet, regex):
    count = []
    entries = re.findall(regex, tweet["doc"]["text"])
    for entry in entries:
        count.append(entry.strip())
    return count

if __name__ == "__main__":
    with open("/Users/jethrolong/Desktop/bigTwitter.json", 'r') as f:
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

        # try tp customize string lines
        # row_indicator = 0
        # records = []
        # # for line in f:
        # #     if not (line.endswith("[\n") or line.endswith("]}\n") or line.endswith("]}")):
        # #         row_indicator += 1
        # #         if line.endswith("}},\n"):
        # #             line = line[:-2]
        # #         else:
        # #             line = line[:-1]
        # #         tweet = json.loads(line)
        #         if tweet["doc"]["coordinates"] is None:
        #             print("None worked #", row_indicator,"  ", tweet)
                # print("#",row_indicator, "  ",line)

                # records.append(json.loads(line))

        # i = 0
        # for each in records:
        #     if each["doc"]["coordinates"]["coordinates"] != each["doc"]["geo"]["coordinates"][::-1]:
        #         print("1st place: ",each["doc"]["coordinates"]["coordinates"])
        #         print("snd place: ",each["doc"]["geo"]["coordinates"][::-1])
        #         print("index: ",i)
        #         break
        #     i += 1

        # try regex to extract hashtags

        # row_indicator = 0
        # for line in f:
        #     if not (line.endswith("[\n") or line.endswith("]}\n") or line.endswith("]}")):
        #         row_indicator += 1
        #         if line.endswith("}},\n"):
        #             line = line[:-2]
        #         else:
        #             line = line[:-1]
        #         tweet = json.loads(line)
        #         hashtag_list = find_hashtags(tweet, HASHTAG_REGEX)
        #         print(hashtag_list)
        #         if row_indicator == 100:
        #             break

        #test on Counter
        hashtag_list = ['melbourne', 'melbourne', 'Melbourne', 'a', 'A']
        count = Counter([x.upper() for x in hashtag_list])
        print(count)












