"""
    This program receives messages from the Food2 queue the RabbitMQ server.
    The program produces an alert when food temperature has stalled (hasn't increased by at least 1 degree in 20 minutes.)  
   
    Author: Samantha Cress
    Based on code by Dr. Denise Case
    Date: February 20, 2023
"""
#Needed Modules
import pika
import sys
import time
from collections import deque

#Declare Constants
host = "localhost"
csv_file = "smoker-temps.csv"
queueF2 = "Food2"
food_alert = 1 

#Food time window is 10 minutes
#At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
#If food temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!
food2_deque = deque(maxlen=20)

def food2_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 02-food-A queue.
    Monitor food A temperature. Send an alert if the temp of food A changes (+/-) 1 F or less in 10 min (or 20 readings). """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} Food1_Temp")
    time.sleep(5)
    # basic_ack - acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

    #Create food1 deque
    #Add message to deque
    food2_deque.append(body.decode())
    #first item in deque (oldest) Will start with first entry from csv file
    food2_message1 = food2_deque[0]
    # split the oldest message in the deque and into list
    # the first list item [0] is date and timestamp from 20 messages ago
    # the second list item [1] is the temp from 20 messages ago
    food2_message1_split = food2_message1.split(", ")
    # change to float 
    food1_message1_temp = float(food2_message1_split[1])

    #Create message for current food temp
    food1_current_temp = body.decode()
    # split the current message and put data into list form
    food1_current_temp_split = food1_current_temp.split(", ")
    # change temp to float
    food1_current_temp1 = float(food1_current_temp_split[1])

    temp_change = round(food1_message1_temp - food1_current_temp1)

    if abs(temp_change) <= food_alert:
        print(F'>>> Food Stall alert!')


def main(host: str, queue: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={host}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        # need one channel per consumer
        channel = connection.channel()

        # use the channel to declare a durable queue (1 per queue)
        # a durable queue will survive a RabbitMQ server restart and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=queueF2, durable=True)

        # The QoS level controls the # of messages that can be in-flight (unacknowledged by the consumer) at any given time.
        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # we use the auto_ack for this assignment
        channel.basic_consume(queue=queueF2, on_message_callback=food2_callback, auto_ack=False)


        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

########################################################

# Run program

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main(host, queueF2)