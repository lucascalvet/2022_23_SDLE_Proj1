import itertools
import logging
import sys
import zmq

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 10
SERVER_ENDPOINT = "tcp://localhost:5555"

if __name__ == '__main__':
    if len(sys.argv) == 2:
        client_id = sys.argv[2]
    else:
        print("usage: python3 client.py <client_id>")

context = zmq.Context()

logging.info("Connecting to server…")
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)

client_id = "luke"
topics = {
    "news": {
        "msg_last_id": -1,
        "pub_count": 0,
    },
    "football": {
        "msg_last_id": -1,
        "pub_count": 0,
    },
}


def put_msg(topic_id, text):
    return "p {} {} {} {}".format(client_id, topic_id, topics[topic_id]["pub_count"] + 1, text)


def put_res(topic_id, response):
    if response[0] == "a":
        topics[topic_id]["pub_count"] = topics[topic_id]["pub_count"] + 1
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
        return -3
    logging.error("Malformed reply from server: %s", ' '.join(response))
    return -1


def get_msg(topic_id):
    return "g {} {} {}".format(client_id, topic_id, topics[topic_id]["msg_last_id"] + 1)


def get_res(topic_id, response):
    if response[0] == 'a':
        topics[topic_id]["msg_last_id"] += 1
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
        to_update = {topic_id: {"msg_last_id": -1, "pub_count": 0}}
        topics.update(to_update)
        logging.info("Successful subscribe")
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
        del topics[topic_id]
        logging.info("Successful unsubscribe")
        return 0
    elif response[0] == 'e':
        logging.warning(
            "Tried to unsubscribe to an already unsubscribed topic")
        return -2
    logging.error("Malformed error reply from server")
    return -1


for sequence in itertools.count():

    request = get_msg("news").encode()
    logging.info("Sending (%s)", request)
    client.send(request)

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
            get_res("news", reply)
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
        client.send(request)

while True:
    request = input("Enter request: ")
