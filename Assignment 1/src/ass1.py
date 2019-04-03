from mpi4py import MPI
from collections import Counter
import json
import math
import sys
import time


def get_Grid(gridFile):
    with open(gridFile, 'r', encoding="utf-8") as melbGrid:
        gridData = json.load(melbGrid)
        grid = []
        for box in gridData["features"]:
            grid.append(box["properties"])
    return grid


def doOperation_on_tweets(tweet_file, grid):
    grid_cor_dict = {}
    grid_hashtag_dict = {}  # {“A1”:[[hashtag1_from_tweet1, hashtag2_from_tweet1],[hashtag1_from_tweet2, hashtag2_from_tweet2]]}
    for each in tweet_file:
        # lookup coordinates
        temp_cor_list = each["value"]["geometry"]["coordinates"]
        temp_hashtags_list = each["doc"]["entities"]["hashtags"]
        temp_hashtags = []
        for entry in temp_hashtags_list:  # a list of hashtags in each tweet
            temp_hashtags.append(entry["text"])  # tweet1 [hashtag1, hashtag2,...], []
        area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)
        # Construct and merge into dictionary -- area : num_tweets
        if area not in grid_cor_dict.keys():
            if area is not None:
                grid_cor_dict[area] = 1
        else:
            grid_cor_dict[area] += 1
        # Construct and merge into dictionary -- {area : [hashtag1, hashtag2, hashtag1,...]}
        if area not in grid_hashtag_dict.keys():
            if area is not None:
                grid_hashtag_dict[area] = temp_hashtags
                # print("here new : ", grid_hashtag_dict)
        else:
            grid_hashtag_dict[area] += temp_hashtags
    return grid_cor_dict, grid_hashtag_dict


def get_FileName(argv):
    if len(argv) > 2:
        return argv[2]
    else:
        return "bigTwitter.json"


def print_result(grid_dict, hashtag_dict):
    print("Results showing: \n")
    print("-------------------------------------------------------------------")
    for each in grid_dict:
        print("Grid {}: {} tweets. Trending hashtags:".format(each[0], each[1]))
        for hashtag in hashtag_dict[each[0]]:
            print("                                        {}".format(hashtag))
    print("-------------------------------------------------------------------")


# Decide which grid box a given point with its x, y coordinates belongs to
def which_grid_box(cor_x, cor_y, grid):
    area = None
    for box in grid:
        if cor_x >= box["xmin"] and cor_x <= box["xmax"] \
                and cor_y >= box["ymin"] and cor_y <= box["ymax"]:
            area = box["id"]
    return area


def order_dict(dict_items):
    sortedDict = sorted(dict_items, reverse=True, key=lambda x: x[-1])
    return sortedDict


def order_hashtags(dict_obj):
    new_dict = {}
    for k, v in dict_obj.items():
        top5_list = order_dict(Counter(v).items())[:5]
        new_dict[k] = top5_list
    return new_dict


def merge_results(dict_obj):
    new_dict = {}
    for entry in dict_obj:
        for k, v in entry.items():
            if k not in new_dict:
                new_dict[k] = v
            else:
                new_dict[k] += v
    return new_dict


def main(argv):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    melbGrid = get_Grid(argv[1])

    grid_cor_dict = {}
    grid_hashtag_dict = {}

    with open(get_FileName(argv), 'r', encoding="utf-8") as f:
        records = json.loads(f.read())["rows"]
        num_records = len(records)
        len_chunks = int(math.ceil(num_records / size))
        start_Row = rank * len_chunks
        if (rank + 1) * len_chunks > num_records:
            grid_cor_dict, gird_hashtag_dict = doOperation_on_tweets(
                records[start_Row:], melbGrid)
            # print("\n Process #{}: with grid_cor_dict{}".format(rank, grid_cor_dict))
            # print("Process #{}: with grid_hashtag_dict{}".format(rank, grid_hashtag_dict))
            grid_cor_dict = comm.gather(grid_cor_dict, root=0)
            grid_hashtag_dict = comm.gather(grid_hashtag_dict, root=0)

        else:
            grid_cor_dict, gird_hashtag_dict = doOperation_on_tweets(
                records[start_Row:(start_Row + len_chunks)], melbGrid)
            # print("\n Process #{}: with grid_cor_dict{}".format(rank, grid_cor_dict))
            # print("Process #{}: with gird_hashtag_dict{}".format(rank, grid_hashtag_dict))
            grid_cor_dict = comm.gather(grid_cor_dict, root=0)
            grid_hashtag_dict = comm.gather(gird_hashtag_dict, root=0)

        comm.barrier()
        if rank == 0:
            # reduction -- gridboxes
            new_grid_dict = merge_results(grid_cor_dict)
            # reduction -- hashtags
            new_hashtag_dict = merge_results(grid_hashtag_dict)

            # Ordering
            ordered_grid_list = order_dict(new_grid_dict.items())
            ordered_hastag_dict = order_hashtags(new_hashtag_dict)

            # results show
            print_result(ordered_grid_list, ordered_hastag_dict)


if __name__ == "__main__":
    # argv = ["/Users/jethrolong/Desktop/melbGrid.json",
    #         "/Users/jethrolong/Desktop/smallTwitter.json"]
    # commi
    time_start = time.time()
    main(sys.argv)
    time_end = time.time()
    print("Total time used: %.3f sec." % (time_end - time_start))
