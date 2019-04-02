'''
import libraries
'''
from mpi4py import MPI
import json
import sys


def get_FileName(argv):
    if len(argv) > 1:
        return argv[1]
    else:
        return "bigTwitter.json"


# load the grid data, specifically the coordinates of vetices
# of each grid box
def get_Grid(gridFile):
    with open(gridFile, 'r') as melbGrid:
        gridData = json.load(melbGrid)
        grid = []
        for box in gridData["features"]:
            grid.append(box["properties"])
    return grid


# count the total number of data records in a json lines file
def count_lines(tweet_file):
    count = 0
    for line in tweet_file:
        count += 1
    return count


def order_num_tweets():
    pass


def order_hashtags():
    pass


# Decide which grid box a given point with its x, y coordinates belongs to
def which_grid_box(cor_x, cor_y, grid):
    area = ""
    for box in grid:
        if cor_x >= box["xmin"] and cor_x <= box["xmax"] \
         and cor_y >= box["ymin"] and cor_y <= box["ymin"]:
            area = box["id"]
    return area


# def data_distribution(comm, grid, tweet_file, grid_dict, grid_hashtag_dict):
#     with open(tweet_file, 'r') as f:
#         tweet = json.loads(f.read())
#         row_indicator = 0
#         for row in tweet["rows"]:
#             recipient = row_indicator % comm.Get_size()
#
#             print("row #", row_indicator, " to be send to ", recipient)
#
#             # comm.send(row, dest=recipient)
#             row_indicator += 1
#             process_tweet(comm, grid, grid_dict, grid_hashtag_dict,
#                           row, recipient)

def distribute_tweet(comm, tweet_file, size, grid, grid_dict, grid_hashtag_dict):
    # rows_for_master = []
    with open(tweet_file, 'r') as f:
        tweet = json.loads(f.read())
        row_indicator = 0
        for row in tweet["rows"]:
            recipient = row_indicator % size
            if row_indicator == 587:
                print("row #587: ", "recipient :", recipient, "  ", row)
            print("row #", row_indicator, " to be send to ", recipient)
            if comm.Get_rank() != recipient:
                comm.send(row, dest=recipient)
            else:
                process_tweet(grid, row, grid_dict, grid_hashtag_dict)
            row_indicator += 1
    comm.bcast("no more tweets", root=0)


# def process_tweet(comm, grid, grid_dict, grid_hashtag_dict, row, recipient):
#     # grid_dict = {}  # {"A1": num_tweets, "A2": num_tweets}
#     # grid_hashtag_dict = {}
#     # with open(tweet_file, 'r') as f:
#     #     tweet = json.load(f)
#     #     row_indicator = 0
#     #     for row in tweet["rows"]:
#     #         recipient = row_indicator % comm.Get_size()
#     #         recvBuf = comm.sendrecv(row, dest=recipient)
#     #         row_indicator += 1
#     recvBuf = comm.Sendrecv(row, dest=recipient)
#     print("Process #", comm.Get_rank(), "receives ", recvBuf)
#     temp_cor_list = recvBuf["value"]["geometry"]["coordinates"]
#     area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)
#     if area not in grid_dict.keys():
#         if area != "":
#             grid_dict[area] = 1
#     else:
#         grid_dict[area] += 1
#     # code for find hashtag
#     # ...
#     grid_dict = comm.gather(grid_dict, root=0)
#     return grid_dict, grid_hashtag_dict

def has_more_tweets(recvBuf):
    if type(recvBuf) == str and recvBuf == "no more tweets":
        return False
    return True


def Get_tweet_row(comm, recvBuf):
    recvBuf = comm.recv(source=0)
    return recvBuf


def process_tweet(grid, tweet_row, grid_dict, grid_hashtag_dict):
    temp_cor_list = tweet_row["value"]["geometry"]["coordinates"]
    area = which_grid_box(temp_cor_list[0], temp_cor_list[1], grid)
    if area not in grid_dict.keys():
        if area != "":
            grid_dict[area] = 1
    else:
        grid_dict[area] += 1
    return grid_dict, grid_hashtag_dict


def main(argv):
    comm = MPI.COMM_WORLD
    # Get rank of process
    rank = comm.Get_rank()
    processes = comm.Get_size()
    grid = get_Grid(argv[0])  # suppose argv == "melbGrid.json"

    grid_dict = {}  # [{"A1":23, "B1":34},{}]
    grid_hashtag_dict = {}

    if processes < 2:
        # to be continue
        pass
    else:
        if rank == 0:
            print("now #0 starts data_distribution: ...\n")
            distribute_tweet(comm, argv[1], processes, grid,
                             grid_dict, grid_hashtag_dict)
            # print("#0 start processes its own tweet record:...\n")
            # grid_dict, grid_hashtag_dict = process_tweet(comm, grid)
            grid_dict = comm.gather(grid_dict, root=0)
        else:
            while(True):
                recvBuf = None
                recvBuf = Get_tweet_row(comm, recvBuf)
                if not has_more_tweets(recvBuf):
                    break
                print("#", comm.Get_rank(), " is in process")

                # keep updating its local dicts
                grid_dict, grid_hashtag_dict = process_tweet(grid, recvBuf,
                                                             grid_dict,
                                                             grid_hashtag_dict)

            grid_dict = comm.gather(grid_dict, root=0)
    comm.barrier()
    if rank == 0:
        new_grid_dict = {}
        for entry in grid_dict:
            for k, v in entry.items():
                if k not in new_grid_dict:
                    new_grid_dict[k] = v
                else:
                    new_grid_dict[k] += v
        print(new_grid_dict)


if __name__ == "__main__":
    argv = ["/Users/jethrolong/Desktop/melbGrid.json",
            "/Users/jethrolong/Desktop/tinyTwitter.json"]
    main(argv)  # should be sys.argv later on
