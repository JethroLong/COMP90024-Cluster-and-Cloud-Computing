from mpi4py import MPI
import json
import math
import sys


def get_Grid(gridFile):
    with open(gridFile, 'r') as melbGrid:
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
           temp_hashtags.append(entry["text"])   # tweet1 [hashtag1, hashtag2,...], []

        area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)
        # Construct and merge into dictionary -- area : num_tweets
        if area not in grid_cor_dict.keys():
            if area is not None:
                grid_cor_dict[area] = 1
        else:
            grid_cor_dict[area] += 1

        # Construct and merge into dictionary -- {area : [hashtag1, hashtag2, hashtag1,...]}

        if area not in grid_cor_dict.keys():
            if area is not None:
                grid_hashtag_dict[area] = temp_hashtags_list
        else:
            grid_hashtag_dict[area] += temp_hashtags_list

    return grid_cor_dict, grid_hashtag_dict


def get_FileName(argv):
    if len(argv) > 1:
        return argv[1]
    else:
        return "bigTwitter.json"


# # count the total number of data records in a json lines file
# def count_Records(tweet_file):
#     count = 0
#     jsonLines = json.loads(tweet_file.read())
#     for line in jsonLines["rows"]:
#         count += 1
#     return count


# Decide which grid box a given point with its x, y coordinates belongs to
def which_grid_box(cor_x, cor_y, grid):
    area = None
    for box in grid:
        if cor_x >= box["xmin"] and cor_x <= box["xmax"] \
         and cor_y >= box["ymin"] and cor_y <= box["ymax"]:
            area = box["id"]
    return area


def main(argv):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    melbGrid = get_Grid(argv[0])
    # print("grid:")
    # for grid in melbGrid:
    #     print(grid)

    grid_cor_dict = {}
    grid_hashtag_dict = {}

    if size > 1:
        with open(argv[1], 'r') as f:
            records = json.loads(f.read())["rows"]
            num_records = len(records)
            len_chunks = int(math.ceil(num_records / size))
            start_Row = rank * len_chunks
            if (rank + 1) * len_chunks > num_records:
                grid_cor_dict, gird_hashtag_dict = doOperation_on_tweets(
                    records[start_Row:], melbGrid)
                print("Process #{}: with {}".format(rank, grid_cor_dict))
                print("Process #{}: with {}".format(rank, grid_hashtag_dict))
                grid_cor_dict = comm.gather(grid_cor_dict, root=0)
            else:
                grid_cor_dict, gird_hashtag_dict = doOperation_on_tweets(
                    records[start_Row:(start_Row + len_chunks)], melbGrid)
                print("Process #{}: with {}".format(rank, grid_cor_dict))
                print("Process #{}: with {}".format(rank, grid_hashtag_dict))
                grid_cor_dict = comm.gather(grid_cor_dict, root=0)

            comm.barrier()
            if rank == 0:
                print(grid_cor_dict)
                new_grid_dict = {}
                for entry in grid_cor_dict:
                    for k, v in entry.items():
                        if k not in new_grid_dict:
                            new_grid_dict[k] = v
                        else:
                            new_grid_dict[k] += v
                print(new_grid_dict)


if __name__ == "__main__":
    argv = ["/Users/huining/Desktop/melbGrid.json",
            "/Users/huining/Desktop/tinyTwitter.json"]
    main(argv)
