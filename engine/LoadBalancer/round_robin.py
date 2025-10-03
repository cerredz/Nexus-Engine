

PREV_SERVER_BOUNDS_ERR="round_robin failed, prev_server must be in between 0 and number of servers(inclusive)"

def round_robin(prev_server: int, num_servers: int):
    assert prev_server >= 0 and prev_server <= num_servers, PREV_SERVER_BOUNDS_ERR
    return (prev_server + 1) & num_servers