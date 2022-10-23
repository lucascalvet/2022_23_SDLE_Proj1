import itertools
import logging
import sys
import zmq
import json

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 10
SERVER_ENDPOINT = "tcp://localhost:5555"


client_id = "luke"
topics ={}
# topics = {
#     "news": {
#         "msg_last_id": -1,
#         "pub_count": 1,
#     },
#     "football": {
#         "msg_last_id": -1,
#         "pub_count": 0,
#     },
# }


def updateJSON():
    with open('mytopics.txt', 'w') as convert_file:
        convert_file.write(json.dumps(topics))

def readJSON():
    global topics
    with open('mytopics.txt') as json_file:
        try:
            topics = json.load(json_file)
        except:
            return

def put_msg(topic_id, text):
    if topic_id not in topics.keys():
        topics[topic_id] = {"msg_last_id": -2, "pub_count": 0}
        updateJSON()
    return "p {} {} {} {}".format(client_id, topic_id, topics[topic_id]["pub_count"] + 1, text)


def put_res(topic_id, response):
    if response[0] == "a":
        topics[topic_id]["pub_count"] = topics[topic_id]["pub_count"] + 1
        updateJSON()
        return 0
    if response[0] == "e":
        if (len(response) != 2):
            logging.error("Malformed error reply from server")
            return -1
        if (response[1] == "ns"):
            logging.error(
                "Put unsuccessful, Server states client is not subscribed")
            return -2
        if (not response[1].isdigit()):
            logging.error("Malformed error reply from server")
            return -1
        # TODO: What to do here?
        logging.error("Put unsuccessful, count mismatch. Updating count")
        topics[topic_id]["pub_count"] = int(response[1])
        updateJSON()
        return -3
    logging.error("Malformed reply from server: %s", ' '.join(response))
    return -1


def get_msg(topic_id):
    return "g {} {} {}".format(client_id, topic_id, topics[topic_id]["msg_last_id"] + 1)


def get_res(topic_id, response):
    if response[0] == 'a':
        topics[topic_id]["msg_last_id"] += 1
        updateJSON()
        logging.info("Successful get")
        print("Message: " + response[2])
        return 0
    if response[0] == 'e':
        if response[1] == "ns":
            logging.warning(
                "Tried to send get command for topic not subscribed")
            return -2
        else:
            if (not response[1].isdigit()):
                logging.error("Malformed error reply from server")
                return -1
            if topics[topic_id]["msg_last_id"] != int(response[1]):
                topics[topic_id]["msg_last_id"] = int(response[1])
                updateJSON()
                logging.warning(
                    "Tried to send get command for inexistent message")
                return -3
            else:
                logging.info('Topic "{%s}" is up to date', topic_id)
                return 0
    logging.error("Malformed error reply from server")
    return -1


def subscribe_msg(topic_id):
    return "s {} {}".format(client_id, topic_id)


def subscribe_res(topic_id, response):
    if response[0] == 'a':
        if (len(response) != 2 or (not response[1].isdigit() and response[1] != "-1")):
            logging.error("Malformed error reply from server")
            return -1
        if topic_id not in topics.keys():
            topics[topic_id] = {"pub_count": 0}
        topics[topic_id]["msg_last_id"] = int(response[1])
        updateJSON()
        logging.info("Successful subscribe. Last id: %s", response[1])
        return 0
    elif response[0] == 'e':
        logging.warning("Tried to subscribe to an already subscribed topic")
        return -2
    logging.error("Malformed error reply from server")
    return -1


def unsubscribe_msg(topic_id):
    return "u {} {}".format(client_id, topic_id)


def unsubscribe_res(topic_id, response):
    if response[0] == 'a':
        topics[topic_id]["message_last_id"] = -2
        updateJSON()
        logging.info("Successful unsubscribe")
        return 0
    elif response[0] == 'e':
        logging.warning(
            "Tried to unsubscribe from an already unsubscribed topic")
        return -2
    logging.error("Malformed error reply from server")
    return -1


def parse_user_input(input):
    request = input.split(maxsplit=3)

    if len(request) < 2:
        logging.warning("[CLIENT] Invalid request")
        return -1

    cmd = request[0]
    topic_id = request[1]

    if cmd == "put":
        if (len(request) < 3):
            logging.warning("[CLIENT] Invalid put request, missing arguments")
            return -1

        text = ' '.join(request[2:len(request)])
        req = put_msg(topic_id, text)

    elif cmd == "get":
        req = get_msg(topic_id)
    elif cmd == "subscribe":
        req = subscribe_msg(topic_id)
    elif cmd == "unsubscribe":
        req = unsubscribe_msg(topic_id)
    return topic_id, req


def print_usage():
    print(client_id)
    print("Usage: \n Options:")
    print("subscribe <topic_id>")
    print("unsubscribe <topic_id>")
    print("get <topic_id>")
    print("put <topic_id> <text>")


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("usage: python3 client.py <client_id>")
        sys.exit()
    client_id = sys.argv[1]

    print_usage()

readJSON()
context = zmq.Context()
logging.info("Connecting to server…")
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)


for sequence in itertools.count():

    # command = input("Input request: ")
    # # Deal with input
    # request = parse_user_input(command)
    # while request == -1:
    #     print_usage()
    #     command = input("Input request: ")
    #     request = parse_user_input(command)

    # logging.info("[CLIENT] Sending (%s)", request)
    # client.send(request.encode())

    #request = unsubscribe_msg("news").encode()

    command = input("Input request: ")
    # Deal with input
    topic, request = parse_user_input(command)
    while request == -1:
        print_usage()
        command = input("Input request: ")
        topic, request = parse_user_input(command)

    client.send(request.encode())

    retries_left = REQUEST_RETRIES
    while True:
        if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
            reply = client.recv()
            logging.info("Server response: {%s}", reply)

            reply = reply.decode().split(maxsplit=2)
            if len(reply) == 0:
                logging.error("Received empty reply from server")
                continue
            retries_left = REQUEST_RETRIES
            if reply[0] == "i":
                logging.error("Server reported invalid request")
                break
            if request[0] == 'p':
                put_res(topic, reply)
            elif request[0] == 'g':
                get_res(topic, reply)
            elif request[0] == 's':
                subscribe_res(topic, reply)
            elif request[0] == 'u':
                unsubscribe_res(topic, reply)
            break

        retries_left -= 1
        logging.warning("No response from server")
        # Socket is confused. Close and remove it.
        client.setsockopt(zmq.LINGER, 0)
        client.close()
        if retries_left == 0:
            logging.error("Server seems to be offline, abandoning")
            sys.exit()

        logging.info("Reconnecting to server…")
        # Create new connection
        context = zmq.Context()
        client = context.socket(zmq.REQ)
        client.connect(SERVER_ENDPOINT)
        logging.info("Resending (%s)", request)
        command = input("Input request: ")
        # Deal with input
        topic, request = parse_user_input(command)
        while request == -1:
            print_usage()
            command = input("Input request: ")
            topic, request = parse_user_input(command)

        client.send(request.encode())
        #client.send(request)

