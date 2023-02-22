"""
    This program receives messages from the Smart_Smoker queue the RabbitMQ server.
    The program produces an alert when smoker temperature has decreased by more than 15 degrees in 10 minutes.  
   
    Author: Samantha Cress
    Based on code by Dr. Denise Case
    Date: Febuary 20, 2023
"""
#Needed modules
import pika
import sys
import time
from collections import deque

#Define constants
host = "localhost"
csv_file = "smoker-temps.csv"
queueS = "Smart_Smoker"
smoker_alert = 15

#Smoker time window is 2.5 minutes
#At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
#If smoker temp decreases by 15 F or more in 2.5 min (or 5 readings)  --> smoker alert
food_window = deque(maxlen=5)

#Define function on getting message from smart_smoker queue and creating alert. 
def smoker_callback(ch, method, properties, body):
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()}: smoker_temp")
    # simulate work
    time.sleep(1)

    # create deque to store given amount of messages (5)
    # add new message to deque
    food_window.append(body.decode())
    smoker_deque = food_window[0] #This will be the first entry in the CSV file
    smoker_deque_split = smoker_deque.split(", ") #Split the first entry
    smoker_deque_temp = float(smoker_deque_split[1]) #Change Temperature to float
    smoker_current_temp = body.decode()
    smoker_current_temp_split = smoker_current_temp.split(", ") #Split the first messeage to seperate time and temperature
    smoker_current_temp = float(smoker_current_temp_split[1])

    #Calculate the temp change, this will be the oldest temp in the deque minus the most current temp in the deque
    smoker_temp_change = round(smoker_deque_temp - smoker_current_temp, 1)

    # Smoker alert
    if smoker_temp_change >= smoker_alert:
        print(f">>> Smoker alert!")

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
        channel.queue_declare(queue=queueS, durable=True)

        # The QoS level controls the # of messages that can be in-flight (unacknowledged by the consumer) at any given time.
        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # we use the auto_ack for this assignment
        channel.basic_consume(queue=queueS, on_message_callback=smoker_callback, auto_ack=True)


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
    main(host, queueS)
