########################################################################################
# COMP90024 Cluster and Cloud Computing -- Assignment 1
########################################################################################
# This ass1.py is a variation of ass1_big.py
# This ass1.py is tailored for tinyTwitter.json and smallTwitter.json
########################################################################################
# @Author: Junyu Long <JethroLong>   Student ID: 871689
# @Collaborator: Huining Li <huiningl>   Student ID: 870935
# Email: longj2@student.unimelb.edu.au   huiningl@student.unimelb.edu.au
# Status: Completed on 05/04/2019
# Project_URL: <https://github.com/JethroLong/COMP90024-Cluster-and-Cloud-Computing.git>
########################################################################################

from mpi4py import MPI
from collections import Counter
import json
import sys
import time
import re

# CONSTANTS
HASHTAG_REGEX = "\s#\S+\s"


# This function takes a tweet record and a regular expression (REGEX) as inputs to
# find all matched patterns -- Hashtags with pattern " #STRING " in this scenario
def find_hashtags(tweet, Regex):
    hashtags = []
    hashtags = re.findall(r'(?=({}))'.format(Regex), tweet["doc"]["text"])
    hashtags = [x.strip() for x in list(set(hashtags))]
    return hashtags


def get_Grid(gridFile):
    with open(gridFile, 'r', encoding="utf-8") as melbGrid:
        gridData = json.load(melbGrid)
        grid = []
        for box in gridData["features"]:
            grid.append(box["properties"])
    return grid


# This function processed one tweet records and possibly classifies it to valid grid box
# hashtags are also extracted from the raw tweet text if applicable
def doOperation_on_tweet(tweet, grid, grid_cor_dict, grid_hashtag_dict):
    if tweet["value"]["geometry"] is None:
        if tweet["doc"]["geo"] is None:
            return
        else:
            temp_cor_list = tweet["doc"]["geo"]["coordinates"][::-1]
    else:
        temp_cor_list = tweet["value"]["geometry"]["coordinates"]

    area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)
    temp_hashtags_list = find_hashtags(tweet, HASHTAG_REGEX)

    # Construct and merge into dictionary -- {area : num_tweets}
    if area not in grid_cor_dict.keys():
        if area is not None:
            grid_cor_dict[area] = 1
    else:
        grid_cor_dict[area] += 1

    # Construct and merge into dictionary -- {area : [hashtag1, hashtag2,...]}
    if area not in grid_hashtag_dict.keys():
        if area is not None:
            grid_hashtag_dict[area] = temp_hashtags_list
    else:
        grid_hashtag_dict[area] += temp_hashtags_list
    return


def get_FileName(argv):
    if len(argv) > 2:
        return argv[2]
    else:
        return "bigTwitter.json"


# This function gives a comprehensive demonstration of the final results of tweet processing
def print_result(grid_dict, hashtag_dict, longest):
    print("\nResults showing:")
    print("___________________________" * (longest + 1))
    print("===========================" * (longest + 1))
    for each in grid_dict:
        print("Grid {}: {:7} tweets. Trending hashtags:   ".format(each[0], each[1]), end="")
        for hashtag in hashtag_dict[each[0]]:
            for tie in hashtag:
                print(str(tie), end=" ")
            print()
            print("".ljust(46, " "), end="")
        print()
    print("===========================" * (longest + 1))


# Decide which grid box a tweet belongs to with its x, y coordinates.
# Two classifiers ensure all tweets that exactly fall on grid boarders won't miss.
# A tweet will be filtered out only when it fails both classification processes.
def which_grid_box(cor_x, cor_y, grid):
    for box in grid:  # implement up-left rule to classify tweets
        if box["xmin"] < cor_x <= box["xmax"] and (box["ymin"] <= cor_y < box["ymax"]):
            return box["id"]
    for box in grid:  # For those who failed the first classifier, do second classification with down-right rule
        if box["xmin"] <= cor_x < box["xmax"] and (box["ymin"] < cor_y <= box["ymax"]):
            return box["id"]
    return None


# Sort a List deriving from dictionary into descending order (based on the value v in [k, v])
def order_dict(dict_items):
    sorted_dict = sorted(dict_items, reverse=True, key=lambda x: x[-1])
    return sorted_dict


# Sort the gird vs. hashstags dictionary into descending order (based on the occurrences of each hashtag)
# and returns a sorted dictionary
# Case-insensitive
def order_hashtags(dict_obj):
    new_dict = {}
    for k, v in dict_obj.items():
        sorted_list = order_dict(Counter([x.upper() for x in v]).items())
        new_dict[k] = sorted_list
    return new_dict


# This function deals with any ties with regard to the occurrences of hashtags in a specific grid box
# It returns required top_n hashtags as a n-element dictionary. Grid box Ids are the keys and hashtags
# with a tie on occurrences are put at the same Top-k level. (top_n is set to 5 as default)
def resolve_tie(sorted_dict_obj, top_n=5):
    top_n_dict = {}
    longest_tie = -1
    if top_n > 0:
        for k, v in sorted_dict_obj.items():
            next_most = v[0][1]  # occurrences of seq[0] --a hashtag
            tie_list = []
            top_count = 0
            top_n_dict[k] = []
            for seq in v:
                if seq[1] == next_most:
                    tie_list.append(seq)
                else:
                    if top_count < top_n:
                        next_most = seq[1]
                        longest_tie = max(longest_tie, len(tie_list))
                        top_n_dict[k].append(tie_list)
                        top_count += 1
                    else:
                        break
                    tie_list = []
                    tie_list.append(seq)
        return top_n_dict, longest_tie


# Extract dictionaries from the list of dictionary, and merge the values of each entry, which the same key, into
# a new dictionary.
def merge_results(list_of_dicts):
    new_dict = {}
    for entry in list_of_dicts:
        for k, v in entry.items():
            if k not in new_dict:
                new_dict[k] = v
            else:
                new_dict[k] += v
    return new_dict


def main(argv):
    time_start = time.time()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Load melbGrid.json
    melbGrid = get_Grid(argv[1])

    # Each process keeps these two dictionaries to record results
    grid_cor_dict = {}
    grid_hashtag_dict = {}

    with open(get_FileName(argv), 'r', encoding="utf-8") as f:
        row_indicator = 0
        for line in f:
            # Check if the current string is valid
            if not (line.endswith("[\n") or line.endswith("]}\n") or line.endswith("]}")):
                if not (line.endswith("[\n") or line.endswith("]}\n")):
                    row_indicator += 1

                    # Each process deals with corresponding tweets, which is associated with their own ranks
                    # Interleaving -- process reacts to a specific row of tweet records only when the expression
                    # (row_indicator % size) evaluates to its rank
                    if rank == (row_indicator % size):
                        if line.endswith("}},\n"):
                            line = line[:-2]
                        else:
                            line = line[:-1]
                        doOperation_on_tweet(json.loads(line), melbGrid, grid_cor_dict, grid_hashtag_dict)

        # Synchronize different processes before MASTER starts final results processing
        comm.barrier()
        time_end = time.time()

        # gather information from other processes
        grid_cor_dict = comm.gather(grid_cor_dict, root=0)
        grid_hashtag_dict = comm.gather(grid_hashtag_dict, root=0)
        time_diff = comm.gather((time_end - time_start), root=0)

        if rank == 0:
            # reduction -- number of tweets in gridboxes
            new_grid_dict = merge_results(grid_cor_dict)

            # reduction -- hashtags in each gridbox
            new_hashtag_dict = merge_results(grid_hashtag_dict)

            # Ordering
            ordered_grid_list = order_dict(new_grid_dict.items())
            ordered_hastag_dict = order_hashtags(new_hashtag_dict)

            # pick top5 --- ties included.
            top5_hashtag_list, longest_tie = resolve_tie(ordered_hastag_dict, 5)

            # results show
            print_result(ordered_grid_list, top5_hashtag_list, longest_tie)
            print("Total time used (average): %.3f sec." % (sum(time_diff) / len(time_diff)) + "\n")


if __name__ == "__main__":
    main(sys.argv)
