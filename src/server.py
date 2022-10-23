from random import randint
import itertools
import logging
import time
import zmq
import json
import os

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

context = zmq.Context()
server = context.socket(zmq.REP)
server.bind("tcp://*:5555")

topics = {}
# topics = {
#     "news": {
#         "messages": {0: {"publisher": "luke", "text": "the queen has finally died"}, 1: {"publisher": "mrs", "text": "there were only 400"}, 2: {"publisher": "zp", "text": "it's working"}},
#         "msg_last_id": 2,
#         "pubs": {"luke": 1, "mrs": 1, "zp": 1},
#         "subs": {"luke": -1},
#     },
# }

"""
Message format:
 Put:
 <- p <publisher_id> <topic_id> <pub_count> <text>
 -> a <subscribers_count>
 -> e <pub_count>
 -> e ns

 Get:
 <- g <subscriber_id> <topic_id> <message_id>
 -> a <latest_topic_id> <message_text>
 -> e <latest_topic_id>
 -> e ns

 Subscribe:
 <- s <subscriber_id> <topic_id>
 -> a <latest_topic_id>
 -> e as

 Unsubscribe:
 <- u <subscriber_id> <topic_id>
 -> a
 -> e ns

 -> i
"""


def updateJSON():
    """Write the topics information to a JSON file"""
    with open('topics.json', 'w') as convert_file:
        convert_file.write(json.dumps(topics))


def readJSON():
    """Read the topics information from a JSON file"""
    global topics
    path = "topics.json"
    if not os.path.exists(path):
        open("topics.json", "w")

    else:
        with open('topics.json') as json_file:
            try:
                topics = json.load(json_file)
            except:
                return


def process_request(request):
    """Processes a request from the client"""
    invalid_response = "i"
    operation = request.split(maxsplit=4)

    if len(operation) == 0:
        logging.warning("Invalid request")
        return invalid_response

    # Put operation
    if operation[0] == "p":
        if (len(operation) < 4):
            logging.warning("Invalid put request, missing arguments")
            return invalid_response
        publisher_id = operation[1]
        topic_id = operation[2]
        if not operation[3].isdigit():
            logging.warning("Invalid put request, id not an int")
            return invalid_response
        count = int(operation[3])
        text = operation[4]
        return put(publisher_id, topic_id, count, text)

    # Get operation
    if operation[0] == "g":
        if (len(operation) < 4):
            logging.warning("Invalid get request, missing arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        if not operation[3].isdigit():
            logging.warning("Invalid get request, id not an int")
            return invalid_response
        message_id = int(operation[3])
        return get(subscriber_id, topic_id, message_id)

    # Subscribe operation
    if operation[0] == "s":
        if (len(operation) != 3):
            logging.warning(
                "Invalid subscribe request, wrong number of arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        return subscribe(subscriber_id, topic_id)

    # Unsubscribe operation
    if operation[0] == "u":
        if (len(operation) != 3):
            logging.warning(
                "Invalid unsubscribe request, wrong number of arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        return unsubscribe(subscriber_id, topic_id)

    return invalid_response


def put(publisher_id, topic_id, count, text):
    """Publishes a message on a topic"""
    # Check if topic exists. Return ns error otherwise
    if topic_id not in topics.keys():
        topics[topic_id] = {
            "messages": {},
            "msg_last_id": -1,
            "pubs": {publisher_id: 0},
            "subs": {},
        }

    if publisher_id not in topics[topic_id]["pubs"].keys():
        topics[topic_id]["pubs"][publisher_id] = 0

    # Check if the client's registered count is different than the one sent. Return error with the registered counter otherwise
    if (topics[topic_id]["pubs"][publisher_id] + 1 != count):
        return "e " + str(topics[topic_id]["pubs"][publisher_id])

    topics[topic_id]["pubs"][publisher_id] = topics[topic_id]["pubs"][publisher_id] + 1

    # Post the message in the topic and return successfully
    next_id = topics[topic_id]["msg_last_id"] + 1
    subscriber_count = len(topics[topic_id]["subs"])
    if subscriber_count > 0:
        topics[topic_id]["messages"][next_id] = {
            "publisher": publisher_id,
            "text": text
        }
    topics[topic_id]["msg_last_id"] = next_id
    updateJSON()
    return "a " + str(subscriber_count)


def get(subscriber_id, topic_id, message_id):
    """Consumes a message from a topic"""
    # Check if topic exists and the client is subscribed to it. Return ns error otherwise
    if not (topic_id in topics.keys() and subscriber_id in topics[topic_id]["subs"].keys()):
        return "e ns"

    if message_id - 1 > topics[topic_id]["subs"][subscriber_id]:
        topics[topic_id]["subs"][subscriber_id] = message_id - 1

    # Check if message requested exists. Return error with the latest message_id otherwise
    if message_id not in topics[topic_id]["messages"].keys():
        return "e " + str(topics[topic_id]["msg_last_id"])

    min_msg_id = min(topics[topic_id]["messages"].keys())
    if min(topics[topic_id]["subs"].values()) > min_msg_id:
        topics[topic_id]["messages"].pop(min_msg_id)

    # Return successfully with the latest message_id and the requested message text
    updateJSON()
    return "a " + str(topics[topic_id]["msg_last_id"]) + " " + str(topics[topic_id]["messages"][message_id]["text"])


def subscribe(subscriber_id, topic_id):
    """Subscribes a topic"""
    # Check if topic exists and if not create new topic
    if (topic_id not in topics.keys()):
        to_update = {topic_id: {"messages": {}, "msg_last_id": -
                                1, "pubs": {}, "subs": {subscriber_id: -1}}}
        topics.update(to_update)
        return "a " + str(-1)

    # Check if already subscribed
    if (subscriber_id in topics[topic_id]["subs"].keys()):
        return "e as"

    # Subscribe to the topic
    topic_last_id = topics[topic_id]["msg_last_id"]
    topics[topic_id]["subs"][subscriber_id] = topic_last_id
    updateJSON()
    return "a " + str(topic_last_id)


def unsubscribe(subscriber_id, topic_id):
    """Unsubscribes a topic"""
    # Check if topic exists and if the client is subscribed
    if not (topic_id in topics.keys() and subscriber_id in topics[topic_id]['subs'].keys()):
        return "e ns"

    # Unsubscribe
    topics[topic_id]['subs'].pop(subscriber_id)

    # Delete every message in the topic if it doesn't have subs
    if len(topics[topic_id]["subs"]) == 0:
        topics[topic_id]["messages"] = {}

    updateJSON()
    return "a"


readJSON()
print("Server started")
for cycles in itertools.count():
    request = server.recv()

    # Simulate various problems, after a few cycles
    # if cycles > 3 and randint(0, 3) == 0:
    #     logging.info("Simulating a crash")
    #     break

    # Simulate CPU overload, after a few cycles
    # if cycles > 3 and randint(0, 3) == 0:
    #     logging.info("Simulating CPU overload")
    #     time.sleep(2)

    logging.info("Normal request (%s)", request)
    time.sleep(1)  # Do some heavy work

    response = process_request(request.decode())

    time_stamp = time.time()
    print(topics)
    logging.info("Response ({%d}): {%s}", time_stamp, response)
    server.send(response.encode())
