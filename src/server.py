from random import randint
import itertools
import logging
import time
from urllib import response
import zmq

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

context = zmq.Context()
server = context.socket(zmq.REP)
server.bind("tcp://*:5555")

topics = {
    "news": {
        "messages": {0: {"author": "luke", "text": "the queen has finally died"}},
        "msg_last_id": 0,
        "subs": {"luke": 1, "ze": 0},
    }
}


def put(subscriber_id, topic_id, count, text):
    # Check if topic exists and the client is subscribed to it. Return ns error otherwise
    if not (topic_id in topics.keys() and subscriber_id in topics[topic_id]["subs"].keys()):
        return "e ns"

    # Check if the client's count is bigger than the one registered. Return error with the registered counter otherwise 
    if (topics[topic_id]["subs"][subscriber_id] >= count):
        return "e " + str(topics[topic_id]["subs"][subscriber_id])

    # Post the message in the topic and return successfully
    next_id = topics[topic_id]["msg_last_id"] + 1
    topics[topic_id]["messages"][next_id] = {
        "author": subscriber_id, "text": text}
    topics[topic_id]["msg_last_id"] = next_id
    return "a"


def get(subscriber_id, topic_id, message_id):
    # Check if topic exists and the client is subscribed to it. Return ns error otherwise
    if not (topic_id in topics.keys() and subscriber_id in topics[topic_id]["subs"].keys()):
        return "e ns"

    # Check if message requested exists. Return error with the latest message_id otherwise
    if message_id not in topics[topic_id]["messages"].keys():
        return "e " + str(topics[topic_id]["msg_last_id"])

    # Return successfully with the latest message_id and the requested message text
    return "a " + str(topics[topic_id]["msg_last_id"]) + " " + str(topics[topic_id]["messages"][message_id]["text"])


def subscribe(subscriber_id, topic_id):
    # Check if topic exists and if not create new topic
    if (topic_id not in topics.keys()):
        to_update = {topic_id: {"messages": {}, "msg_last_id": -1, "subs": {}}}
        topics.update(to_update)
    
    # Check if already subscribed
    if (subscriber_id in topics[topic_id]["subs"].keys()):
        # Return error? Or everything is fine? (I vote everything is fine -Fred)
        return "e as"

    # Subscribe to the topic
    new_subs = {subscriber_id: 0}
    topics[topic_id]["subs"].update(new_subs)
    return "a"


def unsubscribe(subscriber_id, topic_id):
    # Check if it's subscribed
    if (subscriber_id not in topics[topic_id]['subs'].keys()):
        return "e ns"

    # Unsubscribe
    topics[topic_id]['subs'].pop(subscriber_id)
    # del topics[topic_id]['subs'][subscriber_id]
    return "a"

"""
Loop for receiving client messages
Message format:
 Put:
 <- p <subscriber_id> <topic_id> <count> <text>
 -> a
 -> e <count>
 -> e ns
 
 Get:
 <- g <subscriber_id> <topic_id> <message_id>
 -> a <latest_topic_id> <message_text>
 -> e <latest_topic_id>
 -> e ns

 Subscribe:
 <- s <subscriber_id> <topic_id>
 -> a
 -> e as

 Unsubscribe:
 <- u <subscriber_id> <topic_id>
 -> a
 -> e ns

 -> i
"""

def process_request(request):
    invalid_response = "i"
    operation = request.split(maxsplit=5)

    if len(operation == 0):
        logging.warning("Invalid request")
        return invalid_response

    elif operation[0] == "p":
        if(len(operation) < 4):
            logging.warning("Invalid put request, missing arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        if not operation[3].isdigit():
            logging.warning("Invalid put request, id not an int")
            return invalid_response
        count = operation[3]
        text = operation[4]
        return put(subscriber_id, topic_id, count, text)
        
    elif operation[0] == "g":
        if(len(operation) < 4):
            logging.warning("Invalid get request, missing arguments")
            return invalid_response
        subscriber_id = int(operation[1])
        topic_id = operation[2]
        if not operation[3].isdigit():
            logging.warning("Invalid get request, id not an int")
            return invalid_response
        message_id = int(operation[3])
        return get(subscriber_id, topic_id, message_id)

    elif operation[0] == "s":
        if(len(operation) != 3):
            logging.warning("Invalid subscribe request, wrong number of arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        return subscribe(subscriber_id, topic_id)

    elif operation[0] == "u":
        if(len(operation) != 3):
            logging.warning("Invalid unsubscribe request, wrong number of arguments")
            return invalid_response
        subscriber_id = operation[1]
        topic_id = operation[2]
        return unsubscribe(subscriber_id, topic_id)

    return invalid_response

for cycles in itertools.count():
    request = server.recv()

    # Simulate various problems, after a few cycles
    if cycles > 3 and randint(0, 3) == 0:
        logging.info("Simulating a crash")
        break
        
    elif cycles > 3 and randint(0, 3) == 0:
        logging.info("Simulating CPU overload")
        time.sleep(2)

    logging.info("Normal request (%s)", request)
    time.sleep(1)  # Do some heavy work

    response = process_request(request)

    server.send(response)
