from mpi4py import MPI
from collections import Counter
import json
import sys
import time


def get_Grid(gridFile):
    with open(gridFile, 'r', encoding="utf-8") as melbGrid:
        gridData = json.load(melbGrid)
        grid = []
        for box in gridData["features"]:
            grid.append(box["properties"])
    return grid


def doOperation_on_tweet(tweet, grid, grid_cor_dict, grid_hashtag_dict):
    temp_cor_list = []
    if tweet["doc"]["coordinates"] is None:
        if tweet["doc"]["geo"] is None:
            return
        else:
            temp_cor_list = tweet["doc"]["geo"]["coordinates"][::-1]
    else:
        temp_cor_list = tweet["doc"]["coordinates"]["coordinates"]
    area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)

    # {“A1”:[[hashtag1_from_tweet1, hashtag2_from_tweet1],[hashtag1_from_tweet2, hashtag2_from_tweet2]]}
    temp_hashtags_list = tweet["doc"]["entities"]["hashtags"]
    temp_hashtags = []
    for entry in temp_hashtags_list:  # a list of hashtags in each tweet
        temp_hashtags.append(entry["text"])  # tweet1 [hashtag1, hashtag2,...], []
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
    return


def get_FileName(argv):
    if len(argv) > 2:
        return argv[2]
    else:
        return "bigTwitter.json"


def print_result(grid_dict, hashtag_dict):
    print("Results showing: \n")
    print("===================================================================")
    for each in grid_dict:
        print("Grid {}: {} tweets. Trending hashtags:".format(each[0], each[1]))
        for hashtag in hashtag_dict[each[0]]:
            print("                                        {}".format(hashtag))
    print("===================================================================")
    print("___________________________________________________________________")


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

    # Each process keeps these two dictionaries
    grid_cor_dict = {}
    grid_hashtag_dict = {}

    with open(get_FileName(argv), 'r', encoding="utf-8") as f:
        row_indicator = 0
        for line in f:
            if not (line.endswith("[\n") or line.endswith("]}\n") or line.endswith("]}")):
                row_indicator += 1
                if rank == (row_indicator % size):
                    if line.endswith("}},\n"):
                        line = line[:-2]
                    else:
                        line = line[:-1]
                    doOperation_on_tweet(json.loads(line), melbGrid, grid_cor_dict, grid_hashtag_dict)

        grid_cor_dict = comm.gather(grid_cor_dict, root=0)
        grid_hashtag_dict = comm.gather(grid_hashtag_dict, root=0)

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
