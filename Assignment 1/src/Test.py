import csv
import json
import re
import ijson
from collections import Counter

HASHTAG_REGEX = "\s{1}#\S+\s{1}"

def find_hashtags(tweet, regex):
    count = []
    entries = re.findall(regex, tweet["doc"]["text"])
    for entry in entries:
        count.append(entry.strip())
    return count


# {"A1": [('tag1', 10), ('tag2', 9), ('tag3', 8), ('tag4', 7), ('tag5', 6), ("tag6", 2)],
# "B1": [('tag1', 7), ('tag2', 7), ('tag3', 6), ('tag4', 5), ('tag5', 4), ("tag6", 3), ("tag7", 2), ("tag8", 1)]}

def resolve_tie(sorted_dict_obj):
    top5_dict = {}
    longest_tie = -1
    for k, v in sorted_dict_obj.items():
        next_most = v[0][1]  # occurrences of seq[0] --a hashtag
        tie_list = []
        top_count = 0
        top5_dict[k] = []
        for seq in v:
            if seq[1] == next_most:
                tie_list.append(seq)
            else:
                if top_count < 5:
                    next_most = seq[1]
                    longest_tie = max(longest_tie, len(tie_list))
                    top5_dict[k].append(tie_list)
                    top_count += 1
                else:
                    break
                tie_list = []
                tie_list.append(seq)
    return top5_dict, longest_tie


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
        #         if row_indicator == 100000:
        #             break

        #test on Counter
        # hashtag_list = ['melbourne', 'melbourne', 'Melbourne', 'a', 'A']
        # count = Counter([x.upper() for x in hashtag_list])
        # print(count)

        # hashtag_dict = {"A1": [('tag1', 10), ('tag2', 9), ("tagss", 9), ('tag3', 8), ('tag4', 7), ('tag5', 6), ("tag6", 2)],
        #                 "B1": [('tag1', 7), ('tag2', 7), ('tag3', 6), ('tag4', 5), ('tag5', 4), ("tag6", 3), ("tag7", 2), ("tag8", 1)]
        #                 }
        # top5, longest= resolve_tie(hashtag_dict)
        # for k, v in top5.items():
        #     print("Grid {}, tags:  ".format(k))
        #     for each in v:
        #         print("             {}".format(each))
        # print(longest)

        #try findall()
        pattern = re.compile(HASHTAG_REGEX)
        text = " #tag1 #tag2 #tag1 #tag2 #tag3 #tag2#tag1 #tag2#tag1#tag2. #tag1"
        text = text.replace(" #","  #")
        # text= re.findall("\s{1}#\S+\s{1}", text)
        list_a = re.findall(r'(?=({}))'.format(HASHTAG_REGEX), text)
        list_a = set([x.strip() for x in list_a])
        # text = text.split(" ")
        print(list_a)
        # text = set(text)
        # hashtag = []
        # for entry in text:
        #     if entry.startswith("#"):
        #         hashtag.append(entry)
        # # matches = re.finditer(pattern, text)
        # # list_a = re.findall(HASHTAG_REGEX, text)
        # # list_b = re.split(HASHTAG_REGEX, text)
        # # list_c = re.fullmatch(HASHTAG_REGEX, text)
        # # list_d = [match.group() for match in matches]
        #
        # print(hashtag)

        # print(re.finditer(HASHTAG_REGEX, text))









